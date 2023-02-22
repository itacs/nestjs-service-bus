import { isNil } from '@nestjs/common/utils/shared.utils';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';
import { connectable, defer, mergeMap, Observable, Subject, throwError } from 'rxjs';

import { AzureServiceBusSenderOptions } from '../interfaces';
import { InvalidMessageException } from '../errors';
import { ServiceBusMessage, ServiceBusMessageBatch } from '@azure/service-bus';
import { AmqpAnnotatedMessage } from '@azure/core-amqp';
import { NotImplementedException } from '@nestjs/common';

export abstract class AzureServiceBusClientProxy extends ClientProxy {
	public send<
		TResult = AzureServiceBusSenderOptions,
		TInput = ServiceBusMessage | ServiceBusMessage[] | ServiceBusMessageBatch | AmqpAnnotatedMessage | AmqpAnnotatedMessage[]
	>(pattern: AzureServiceBusSenderOptions, data: TInput): Observable<TResult> {
		return throwError(() => new NotImplementedException("ServiceBus Transporter does not support replies!"));
	}

	protected publish(packet: ReadPacket<any>, callback: (packet: WritePacket<any>) => void): () => void {
		throw new NotImplementedException("ServiceBus Transporter does not support replies!");
	}

	public emit<
		TResult = AzureServiceBusSenderOptions,
		TInput = ServiceBusMessage | ServiceBusMessage[] | ServiceBusMessageBatch | AmqpAnnotatedMessage | AmqpAnnotatedMessage[]
	>(pattern: AzureServiceBusSenderOptions, data: TInput): Observable<TResult> {
		if (isNil(pattern) || isNil(data)) {
			return throwError(() => new InvalidMessageException());
		}
		const source = defer(async () => this.connect()).pipe(
			mergeMap(() => {
				return this.dispatchEvent({ pattern, data });
			})
		);
		const connectableSource = connectable(source, {
			connector: () => new Subject(),
			resetOnDisconnect: false
		});
		connectableSource.connect();
		return connectableSource;
	}
}
