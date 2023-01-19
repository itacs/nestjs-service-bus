import { Injectable, Scope } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, HealthIndicatorResult } from '@nestjs/terminus';
import { AzureServiceBusServer } from '../server';

export interface ServiceBusHealthIndicatorOptions {}

@Injectable({ scope: Scope.TRANSIENT })
export class ServiceBusHealthIndicator extends HealthIndicator {
	/**
	 * Initializes the health indicator
	 */
	constructor() {
		super();
	}

	private async checkServiceBus(options: ServiceBusHealthIndicatorOptions): Promise<boolean> {
		const server = AzureServiceBusServer.instance;
		if (!server) {
			return false;
		}
		return !server.hasClosedConnections();
	}

	/**
	 * Checks if the given microservice is up
	 * @param key The key which will be used for the result object
	 * @param options The options of the microservice
	 *
	 * @throws {HealthCheckError} If the microservice is not reachable
	 *
	 */
	async pingCheck(key: string, options: ServiceBusHealthIndicatorOptions): Promise<HealthIndicatorResult> {
		let isHealthy = false;
		isHealthy = await this.checkServiceBus(options);

		if (!isHealthy) {
			const errorMsg = `Service-Bus Server has closed connections!`;
			throw new HealthCheckError(errorMsg, this.getStatus(key, false, { message: errorMsg }));
		}

		return this.getStatus(key, isHealthy);
	}
}
