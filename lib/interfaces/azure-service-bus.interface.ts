import { CreateQueueOptions, CreateSubscriptionOptions, CreateTopicOptions, OperationOptionsBase, ServiceBusClientOptions } from '@azure/service-bus';

export interface AzureServiceBusOptions {
	connectionString: string;
	groupId: string;
	options?: ServiceBusClientOptions;
	createQueueOptions?: CreateQueueOptions;
	createTopicOptions?: CreateTopicOptions;
	createSubscriptionOptions?: CreateSubscriptionOptions;
}

export interface AzureServiceBusSenderOptions {
	name: string;
	options?: OperationOptionsBase;
}
