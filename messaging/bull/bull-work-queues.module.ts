import { MiddlewareConsumer, Module, forwardRef } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { PixerLoggerModule } from 'src/pixer-logger/pixer-logger.module';
import { WithdrawModule } from 'src/withdraw/withdraw.module';
import { BullWorkQueuesPort } from 'src/core/ports/bull-work-queues.port';
import { PixerLoggerService } from 'src/pixer-logger/pixer-logger.service';
import { BullWorkQueueConfig, BullWorkQueueConfigSchema } from './schemas/bull-work-queue-config';
import { BullWorkQueuesProvider } from './bull-work-queues.provider';
import { HttpMiddleware } from 'src/middlewares/http.middleware';
import { BullWorkQueuesController } from './bull-work-queues.controller';

@Module({
  imports: [
    MongooseModule.forFeature([
      {
        name: BullWorkQueueConfig.name,
        schema: BullWorkQueueConfigSchema,
      },
    ]),
    forwardRef(() => WithdrawModule),
    forwardRef(() => PixerLoggerModule),
  ],
  providers: [PixerLoggerService, BullWorkQueuesProvider],
  exports: [BullWorkQueuesProvider, BullWorkQueuesPort],
})
export class BullWorkQueuesModule {
  configure(consumer: MiddlewareConsumer) {
    consumer.apply(HttpMiddleware).forRoutes(BullWorkQueuesController);
  }
}
