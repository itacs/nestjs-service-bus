import { Module } from "@nestjs/common";
import { ServiceBusHealthIndicator } from "./azure-service-bus.health";

@Module({
  providers: [ServiceBusHealthIndicator],
  exports:[ServiceBusHealthIndicator]
})
export class ServiceBusHealthIndicatorModule {}