import { Server, CustomTransportStrategy, ReadPacket } from '@nestjs/microservices';
import {
	MessageHandlers,
	ProcessErrorArgs,
	ServiceBusAdministrationClient,
	ServiceBusClient,
	ServiceBusReceivedMessage,
	TopicProperties,
	WithResponse,
	SubscriptionProperties,
	isServiceBusError,
	delay,
	ServiceBusReceiver,
	RuleProperties
} from '@azure/service-bus';

import { RestError } from '@azure/core-http';
import AsyncLock from 'async-lock';

import { AzureServiceBusContext } from '../azure-service-bus.context';
import { AzureServiceBusOptions } from '../interfaces';
// import { SbSubscriberMetadata } from '../metadata';
import { Logger } from '@nestjs/common';

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
	private createdReceiver: ServiceBusReceiver | undefined;
	private createdSubscription: SubscriptionWrapper | undefined;
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
		if (!this.sbClient) {
			throw Error('ServiceBus Client not created!');
		}

		if (this.createdReceiver || this.createdSubscription) {
			throw Error('Receiver or Subscription already created!');
		}

		const topic = this.options.topic;
		const subscription = this.options.groupId;

		await this.lock.acquire(['create', topic], async () => {
			await this.ensureTopic(topic);
		});

		//try to avoid conflict error in race condition
		await this.lock.acquire(['create', topic, subscription], async () => {
			// create request channel
			return await this.ensureSubscription(topic, subscription);
		});

		const prepare = async (pattern: string) => {
			await this.lock.acquire(['create', topic, subscription, pattern], async () => {
				// create request channel
				return await this.ensureSubscriptionRule(topic, subscription, pattern);
			});
		};

		const registeredPatterns = [
			...Array.from(this.messageHandlers.keys()).filter((p) => {
				return this.messageHandlers.get(p)?.isEventHandler;
			})
		];
		await Promise.all(registeredPatterns.map(prepare));

		this.createdReceiver = this.sbClient.createReceiver(topic, subscription, this.options.receiverOptions);
		this.createdSubscription = new SubscriptionWrapper();
		this.createdSubscription.subscription = this.createdReceiver.subscribe(this.createMessageHandlers(this.createdSubscription), this.options.subscribeOptions);
		this.log.log(`receiver for ${topic}.${subscription} started`);
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
	private async ensureSubscription(topicName: string, subscriptionName: string): Promise<void> {
		// check if svcName is set
		if (!topicName || !subscriptionName) throw Error('topicName and subscriptionName must be set!');
		let s: WithResponse<SubscriptionProperties>;
		if (!this.sbAdminClient) {
			throw Error('ServiceBus Admin not created!');
		}

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
	}

	private async ensureSubscriptionRule(topicName: string, subscriptionName: string, filterMethod: string) {
		let r: RuleProperties;
		if (!this.sbAdminClient) {
			throw Error('ServiceBus Admin not created!');
		}
		//clean up filterMethod for rule name
		const ruleName = filterMethod.replace(/[^A-Za-z0-9\w-\.\/\~]/g, '-').replace(/^\-+|\-+$/, '');
		try {
			r = await this.sbAdminClient.getRule(topicName, subscriptionName, ruleName);
			this.log.log(`rule '${topicName}' < '${subscriptionName}'.'${ruleName}' found`);
		} catch (ex) {
			const restError = ex as RestError;
			if (restError.code == 'MessageEntityNotFoundError') {
				this.log.log(`rule '${topicName}' < '${subscriptionName}'..'${ruleName}' creating`);
				r = await this.sbAdminClient.createRule(topicName, subscriptionName, ruleName, {
					applicationProperties: { 'x-method': filterMethod }
				});
				this.log.log(`rule '${topicName}' < '${subscriptionName}'..'${ruleName}' created`);
			} else {
				throw ex;
			}
			try {
				// delete default Rule
				await this.sbAdminClient.deleteRule(topicName, subscriptionName, '$Default');
			} catch (ex) {}
		}
	}

	public createMessageHandlers = (subscriptionWrapper: SubscriptionWrapper): MessageHandlers => ({
		processMessage: async (receivedMessage: ServiceBusReceivedMessage) => await this.handleMessage(receivedMessage),
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

	public async handleMessage(receivedMessage: ServiceBusReceivedMessage): Promise<void> {
		const pattern = receivedMessage.applicationProperties?.['x-method'];
		const partialPacket = { data: receivedMessage.body, pattern };
		const packet = (await this.deserializer.deserialize(partialPacket)) as ReadPacket<any>;

		const sbContext = new AzureServiceBusContext([packet.pattern, packet.data, receivedMessage]);
		return this.handleEvent(packet.pattern, packet, sbContext);
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
		return !!((this.createdSubscription?.isClosed ?? true) || (this.createdReceiver?.isClosed ?? true));
	}

	async close(): Promise<void> {
		await this.createdSubscription?.close();
		this.createdSubscription = undefined;
		await this.createdReceiver?.close();
		this.createdReceiver = undefined;
		this.sbAdminClient = undefined;
		await this.sbClient?.close();
	}
}
