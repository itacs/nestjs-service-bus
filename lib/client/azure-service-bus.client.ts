import { MessageHandlers, ProcessErrorArgs, ServiceBusClient, ServiceBusMessage, ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';

import { PacketId, ReadPacket, WritePacket } from '@nestjs/microservices';
import { Injectable, Logger } from '@nestjs/common';

import { AzureServiceBusClientProxy } from '.';
import { AzureServiceBusOptions, AzureServiceBusSenderOptions } from '../interfaces';

@Injectable()
export class AzureServiceBusClient extends AzureServiceBusClientProxy {
	private sbClient: ServiceBusClient;
	private log = new Logger(AzureServiceBusClient.name);

	constructor(protected readonly options: AzureServiceBusOptions) {
		super();

		this.initializeSerializer(options);
		this.initializeDeserializer(options);
	}

	connect(): Promise<any> {
		if (!this.sbClient) {
			this.sbClient = this.createServiceBusClient();
		}

		return Promise.resolve();
	}

	protected async dispatchEvent(partialPacket: ReadPacket<{ pattern: { name: string; options: AzureServiceBusSenderOptions }; data: any }>): Promise<any> {
    const packet = this.assignPacketId(partialPacket);
		const pattern = this.normalizePattern(packet.pattern);
		const { name, options } = JSON.parse(pattern) as AzureServiceBusSenderOptions;
		const serializedPacket = this.serializer.serialize(packet.data);

    let messages = [
      {
        messageId: packet.id,
        ...serializedPacket,
      }
    ];

		const sender = this.sbClient.createSender(name);

    this.log.verbose(`emitting events id:${packet.id} to:${sender.entityPath}`);

		await sender.sendMessages(messages, options);
		await sender.close();
	}

	protected publish(partialPacket: ReadPacket<any>, callback: (packet: WritePacket<any>) => void): () => void {
		try {
			const packet = this.assignPacketId(partialPacket);
			const pattern = this.normalizePattern(packet.pattern);
			const { name, options } = JSON.parse(pattern) as AzureServiceBusSenderOptions;
			const serializedPacket = this.serializer.serialize(packet.data);
			const replyTo = this.getReplyTo(name);
			const sender = this.sbClient.createSender(name);
			const receiver = this.sbClient.createReceiver(replyTo, { receiveMode: 'peekLock' });

			let messages = [
				{
					messageId: packet.id,
					...serializedPacket,
					replyTo
				}
			];

			this.routingMap.set(packet.id, callback);

			this.log.verbose(`sending messages id:${packet.id} to:${sender.entityPath}`);

			sender.sendMessages(messages, options);

			if (replyTo) {
				receiver.subscribe(this.createMessageHandlers(packet, receiver));
			}

			return () => {
				sender.close();
				receiver.close();
				this.routingMap.delete(packet.id);
			};
		} catch (err) {
			callback({ err });
		}
	}

	public createMessageHandlers = (packet: ReadPacket<any> & PacketId, receiver: ServiceBusReceiver): MessageHandlers => ({
		processMessage: async (receivedMessage: ServiceBusReceivedMessage) => {
			await this.handleMessage(receivedMessage, packet, receiver);
		},
		processError: async (args: ProcessErrorArgs): Promise<void> => {
			return new Promise<void>(() => {
				throw new Error(`Error processing message: ${args.error}`);
			});
		}
	});

	public async handleMessage(receivedMessage: ServiceBusReceivedMessage, packet: ReadPacket<any> & PacketId, receiver: ServiceBusReceiver): Promise<void> {
		const { id, isDisposed } = await this.deserializer.deserialize(packet);
		const { body, correlationId, replyTo } = receivedMessage;
		this.log.verbose(`got reply message correlationId: ${correlationId}`);

		if (replyTo || id !== correlationId) {
			this.log.verbose(`skip reply replyTo:${replyTo} id:${id} correlationId:${id}`);
			await receiver.abandonMessage(receivedMessage);
			return;
		}

		const callback = this.routingMap.get(id);
		if (!callback) {
			this.log.verbose(`no reply callback for id:${id}`);
			return;
		}

		if (isDisposed) {
			this.log.verbose(`calling callback (d) for id:${id}`);
			callback({
				response: body,
				isDisposed: true
			});
		}
		this.log.verbose(`calling callback for id:${id}`);
		callback({
			response: body
		});
	}

	public getReplyTo = (pattern: string): string => {
		return `${pattern}.reply`;
	};

	createServiceBusClient(): ServiceBusClient {
		const { connectionString, options } = this.options;
		return new ServiceBusClient(connectionString, options);
	}

	async close(): Promise<void> {
		await this.sbClient?.close();
	}
}
