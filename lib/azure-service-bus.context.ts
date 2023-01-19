import { ServiceBusMessage } from '@azure/service-bus';
import { BaseRpcContext } from '@nestjs/microservices/ctx-host/base-rpc.context';

type AzureServiceBusContextArgs = [string, ServiceBusMessage];

export class AzureServiceBusContext extends BaseRpcContext<AzureServiceBusContextArgs> {
	constructor(args: AzureServiceBusContextArgs) {
		super(args);
	}

	getTopic(): string {
		return this.args[0];
	}

	getServiceBusMessage(): ServiceBusMessage {
		return this.args[1];
	}
}
