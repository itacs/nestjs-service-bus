import {
	CreateQueueOptions,
	CreateSubscriptionOptions,
	CreateTopicOptions,
	OperationOptionsBase,
	ServiceBusClientOptions,
	ServiceBusReceiverOptions,
	SubscribeOptions
} from '@azure/service-bus';

export interface AzureServiceBusOptions {
	connectionString: string;
	topic: string;
	groupId: string;
	options?: ServiceBusClientOptions;
	subscribeOptions?: SubscribeOptions;
	receiverOptions?: ServiceBusReceiverOptions;
	createTopicOptions?: CreateTopicOptions;
	createSubscriptionOptions?: CreateSubscriptionOptions;
}

export interface AzureServiceBusSenderOptions {
	name: string;
	options?: OperationOptionsBase;
}
