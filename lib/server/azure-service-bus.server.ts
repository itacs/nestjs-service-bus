import { Server, WritePacket, CustomTransportStrategy, ReadPacket } from '@nestjs/microservices';
import {
	MessageHandlers,
	ProcessErrorArgs,
	ServiceBusAdministrationClient,
	ServiceBusClient,
	ServiceBusMessage,
	ServiceBusReceivedMessage,
	TopicProperties,
	WithResponse,
	SubscriptionProperties,
	QueueProperties,
	isServiceBusError,
	delay,
	ServiceBusReceiver,
	RuleProperties
} from '@azure/service-bus';

import { RestError } from '@azure/core-http';
import AsyncLock from 'async-lock';

import { AzureServiceBusContext } from '../azure-service-bus.context';
import { AzureServiceBusOptions } from '../interfaces';
import { SbSubscriberMetadata } from '../metadata';
import { Logger } from '@nestjs/common';
import { getSubscriptionName, splitPattern } from '../utils';

interface IPublisherRequest {
	topic: string;
	method?: string;
	replyTo: string;
	correlationId: string;
}

interface ISubscription {
	close(): Promise<void>;
}

class SubscriptionWrapper {
	#isClosed?: boolean;
	public get isClosed(): boolean | undefined {
		return this.#isClosed;
	}
	#subscription?: ISubscription;
	public set subscription(val: ISubscription) {
		this.#subscription = val;
		this.#isClosed = false;
	}
	public async close(): Promise<void> {
		await this.#subscription?.close();
		this.#subscription = undefined;
		this.#isClosed = true;
	}
}

export class AzureServiceBusServer extends Server implements CustomTransportStrategy {
	private sbClient: ServiceBusClient | undefined;
	private sbAdminClient: ServiceBusAdministrationClient | undefined;
	private log = new Logger(AzureServiceBusServer.name);
	private knownQueues: Array<string> = [];
	private createdReceivers: Array<ServiceBusReceiver> = [];
	private createdSubscriptions: Array<SubscriptionWrapper> = [];
	static #instance: AzureServiceBusServer;
	private lock: AsyncLock;
	public static get instance() {
		return AzureServiceBusServer.#instance;
	}

	constructor(protected readonly options: AzureServiceBusOptions) {
		super();
		this.initializeSerializer(options);
		this.initializeDeserializer(options);
		this.lock = new AsyncLock();
		AzureServiceBusServer.#instance = this;
	}

	async listen(callback: (...optionalParams: unknown[]) => any) {
		try {
			this.sbClient = this.createServiceBusClient();
			this.sbAdminClient = this.createServiceBusAdministrationClient();
			await this.start(callback);
		} catch (err) {
			callback(err);
		}
	}

	async start(callback: (err?: unknown, ...optionalParams: unknown[]) => void): Promise<void> {
		await this.bindEvents();
		callback();
	}

	async bindEvents(): Promise<void> {
		const subscribe = async (pattern: string) => {
			let subscriptionFilterMethod: string | undefined;
			let opt: SbSubscriberMetadata;
			if (!this.sbClient) {
				throw Error('ServiceBus Client not created!');
			}
			try {
				opt = JSON.parse(pattern);
			} catch (ex) {
				// check if pattern might be a json after all
				if (pattern.includes('{') && pattern.includes('}')) {
					throw ex;
				}

				// pattern is just a simple string, now used as topic and possibly method, when separated by dot
				const splittedPattern = splitPattern(pattern);
				subscriptionFilterMethod = splittedPattern.method;

				opt = {
					metaOptions: {
						topic: splittedPattern.topic,
						subscription: this.options.groupId,
						receiveMode: 'peekLock'
					},
					type: 'subscription'
				};
			}

			const {
				metaOptions: { topic, subscription, subQueueType, skipParsingBodyAsJson, receiveMode, options }
			} = opt;

			await this.lock.acquire(['create', topic], async () => {
				await this.ensureTopic(topic);
			});

			//try to avoid conflict error in race condition
			const finalName = await this.lock.acquire(['create', topic, subscription, subscriptionFilterMethod ?? ''], async () => {
				// create reply channel
				await this.ensureSubscription(topic, '', subscriptionFilterMethod, 'reply');
				// create request channel
				return await this.ensureSubscription(topic, subscription, subscriptionFilterMethod, 'request');
			});

			const receiver = this.sbClient.createReceiver(topic, finalName, {
				receiveMode,
				subQueueType,
				skipParsingBodyAsJson
			});

			const subscriptionWrapper = new SubscriptionWrapper();
			this.createdSubscriptions.push(subscriptionWrapper);
			subscriptionWrapper.subscription = receiver.subscribe(this.createMessageHandlers(pattern, subscriptionWrapper), options);
			this.log.log(`receiver for ${topic} started`);
			this.createdReceivers.push(receiver);
		};

		const registeredPatterns = [...this.messageHandlers.keys()];

		await Promise.all(registeredPatterns.map(subscribe));
	}

	/**
	 * Ensures that a queue is available in service bus
	 * @param name
	 */
	private async ensureQueue(name: string): Promise<void> {
		if (!this.knownQueues.includes(name)) {
			let q: WithResponse<QueueProperties>;
			if (!this.sbAdminClient) {
				throw Error('ServiceBus Admin not created!');
			}
			try {
				q = await this.sbAdminClient.getQueue(name);
				this.log.log(`queue '${name}' found`);
			} catch (ex) {
				const restError = ex as RestError;
				if (restError.code == 'MessageEntityNotFoundError') {
					q = await this.sbAdminClient.createQueue(name, {
						...{
							maxSizeInMegabytes: 1024,
							maxDeliveryCount: 10,
							defaultMessageTimeToLive: 'P14D',
							lockDuration: 'PT1M',
							autoDeleteOnIdle: 'P14D',
							duplicateDetectionHistoryTimeWindow: 'PT10M',
							enablePartitioning: false
						},
						...(this.options.createQueueOptions ?? {})
					});
					this.log.log(`queue '${name}' created`);
				} else {
					throw ex;
				}
			}
			this.knownQueues.push(name);
		}
	}

	/**
	 * Ensures that a topic is available in service bus
	 * @param name
	 */
	private async ensureTopic(name: string): Promise<void> {
		let t: WithResponse<TopicProperties>;
		if (!this.sbAdminClient) {
			throw Error('ServiceBus Admin not created!');
		}
		try {
			t = await this.sbAdminClient.getTopic(name);
			this.log.log(`topic '${name}' found`);
		} catch (ex) {
			const restError = ex as RestError;
			if (restError.code == 'MessageEntityNotFoundError') {
				this.log.log(`topic '${name}' creating`);
				t = await this.sbAdminClient.createTopic(name, {
					...{
						duplicateDetectionHistoryTimeWindow: 'PT10M',
						requiresDuplicateDetection: true,
						enablePartitioning: false
					},
					...(this.options.createTopicOptions ?? {})
				});
				this.log.log(`topic '${name}' created`);
			} else {
				throw ex;
			}
		}
	}

	/**
	 * Ensures that a subscription to a topic is available in service bus
	 * @param topicName
	 * @param subscriptionName
	 */
	private async ensureSubscription(topicName: string, svcName: string, filterMethod: string | undefined, direction?: 'request' | 'reply'): Promise<string> {
		// check if svcName or filterMethod is set
		if (!svcName && !filterMethod) return '';
		let s: WithResponse<SubscriptionProperties>;
		if (!this.sbAdminClient) {
			throw Error('ServiceBus Admin not created!');
		}
		const subscriptionName = getSubscriptionName(svcName, filterMethod, direction);
		try {
			s = await this.sbAdminClient.getSubscription(topicName, subscriptionName);
			this.log.log(`subscription '${topicName}' < '${subscriptionName}' found`);
		} catch (ex) {
			const restError = ex as RestError;
			if (restError.code == 'MessageEntityNotFoundError') {
				this.log.log(`subscription '${topicName}' < '${subscriptionName}' creating`);
				s = await this.sbAdminClient.createSubscription(topicName, subscriptionName, {
					...{
						maxDeliveryCount: 50,
						autoDeleteOnIdle: 'P14D',
						defaultMessageTimeToLive: 'P14D',
						deadLetteringOnMessageExpiration: true,
						requiresSession: false,
						lockDuration: 'PT5M'
					},
					...(this.options.createSubscriptionOptions ?? {})
				});
				this.log.log(`subscription '${topicName}' < '${subscriptionName}' created`);
			} else {
				throw ex;
			}
		}
		if (filterMethod) {
			await this.createSubscriptionRule(topicName, svcName, filterMethod, direction);
		}
		return subscriptionName;
	}

	private async createSubscriptionRule(topicName: string, svcName: string, filterMethod: string, direction: 'request' | 'reply' | undefined) {
		let r: RuleProperties;
		if (!this.sbAdminClient) {
			throw Error('ServiceBus Admin not created!');
		}
		const subscriptionName = getSubscriptionName(svcName, filterMethod, direction);
		try {
			r = await this.sbAdminClient.getRule(topicName, subscriptionName, 'ByMethod');
			this.log.log(`rule '${topicName}' < '${subscriptionName}'.ByMethod found`);
		} catch (ex) {
			const restError = ex as RestError;
			if (restError.code == 'MessageEntityNotFoundError') {
				this.log.log(`rule '${topicName}' < '${subscriptionName}'.ByMethod creating`);
				r = await this.sbAdminClient.createRule(topicName, subscriptionName, 'ByMethod', {
					applicationProperties: { 'x-method': filterMethod, 'x-direction': direction ?? 'request' }
				});
				this.log.log(`rule '${topicName}' < '${subscriptionName}'.ByMethod created`);
			} else {
				throw ex;
			}
			try {
				// delete default Rule
				await this.sbAdminClient.deleteRule(topicName, subscriptionName, '$Default');
			} catch (ex) {}
		}
	}

	public createMessageHandlers = (pattern: string, subscriptionWrapper: SubscriptionWrapper): MessageHandlers => ({
		processMessage: async (receivedMessage: ServiceBusReceivedMessage) => await this.handleMessage(receivedMessage, pattern),
		processError: async (args: ProcessErrorArgs): Promise<void> => {
			this.log.error(`Error from source ${args.errorSource} occurred: `, args.error);
			// the `subscribe() call will not stop trying to receive messages without explicit intervention from you.
			if (isServiceBusError(args.error)) {
				switch (args.error.code) {
					case 'MessagingEntityDisabled':
					case 'MessagingEntityNotFound':
					case 'UnauthorizedAccess':
						// It's possible you have a temporary infrastructure change (for instance, the entity being
						// temporarily disabled). The handler will continue to retry if `close()` is not called on the subscription - it is completely up to you
						// what is considered fatal for your program.
						console.log(`An unrecoverable error occurred. ${args.error.code}`, args.error);
						this.handleError(JSON.stringify(args.error));
						await subscriptionWrapper.close();
						break;
					case 'MessageLockLost':
						console.log(`Message lock lost for message`, args.error);
						break;
					case 'ServiceBusy':
						// choosing an arbitrary amount of time to wait.
						await delay(1000);
						break;
				}
			}
		}
	});

	public async handleMessage(receivedMessage: ServiceBusReceivedMessage, pattern: string): Promise<void> {
		const partialPacket = { data: receivedMessage, pattern };
		const packet = (await this.deserializer.deserialize(partialPacket)) as ReadPacket<ServiceBusMessage>;

		const sbContext = new AzureServiceBusContext([packet.pattern, packet.data]);
		if (!receivedMessage.replyTo) {
			// handle emit of event
			return this.handleEvent(packet.pattern, { pattern: packet.pattern, data: packet.data.body }, sbContext);
		}
		// handle send of message
		const { topic, method } = splitPattern(packet.pattern);
		const publish = this.getPublisher({ topic, method, replyTo: receivedMessage.replyTo!, correlationId: receivedMessage.messageId as string });
		const handler = this.getHandlerByPattern(pattern);
		if (handler) {
			const response$ = this.transformToObservable(await handler(receivedMessage.body, sbContext));
			response$ && this.send(response$, publish);
		} else {
			throw Error(`Handle can not be found by Pattern '${pattern}'!`);
		}
	}

	public getPublisher(request: IPublisherRequest) {
		return async (data: WritePacket) => {
			let topicOrQueue: string;
			if (!this.sbClient) {
				throw Error('ServiceBus Client not created!');
			}
			// send to queue if no method was found, otherwise use topic
			if (!request.method) {
				topicOrQueue = request.replyTo;
				await this.ensureQueue(topicOrQueue);
			} else {
				topicOrQueue = request.topic;
			}
			const sender = this.sbClient.createSender(topicOrQueue);
			this.log.verbose(`sending reply to collectionId: ${request.correlationId}`);
			const responseMessage = {
				correlationId: request.correlationId,
				body: data.response,
				applicationProperties: {
					'x-method': request.method,
					'x-direction': 'reply'
				}
			} as ServiceBusMessage;
			await sender.sendMessages([responseMessage]);
			await sender.close();
		};
	}

	createServiceBusClient(): ServiceBusClient {
		const { connectionString, options } = this.options;
		return new ServiceBusClient(connectionString, options);
	}

	createServiceBusAdministrationClient(): ServiceBusAdministrationClient {
		const { connectionString, options } = this.options;
		return new ServiceBusAdministrationClient(connectionString, options);
	}

	public hasClosedConnections(): boolean {
		return !!(this.createdSubscriptions.find((s) => s.isClosed) || this.createdReceivers.find((r) => r.isClosed));
	}

	async close(): Promise<void> {
		await Promise.all(this.createdSubscriptions.map((r) => r.close()));
		await Promise.all(this.createdReceivers.map((r) => r.close()));
		this.createdReceivers.length = 0;
		this.knownQueues.length = 0;
		this.sbAdminClient = undefined;
		await this.sbClient?.close();
	}
}
