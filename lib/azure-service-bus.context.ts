import { ServiceBusMessage } from '@azure/service-bus';
import { BaseRpcContext } from '@nestjs/microservices/ctx-host/base-rpc.context';

type AzureServiceBusContextArgs = [string, any, ServiceBusMessage];

export class AzureServiceBusContext extends BaseRpcContext<AzureServiceBusContextArgs> {
	constructor(args: AzureServiceBusContextArgs) {
		super(args);
	}

	getPattern(): string {
		return this.args[0];
	}

	getData(): any {
		return this.args[1];
	}

	getServiceBusMessage(): ServiceBusMessage {
		return this.args[2];
	}
}
