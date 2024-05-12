import { QueuePrefixes } from 'src/infrastructure/adapters/messaging/bull/enums/queue-prefixes.enum';
import { BullWorkQueueConfig } from 'src/infrastructure/adapters/messaging/bull/schemas/bull-work-queue-config';

export type GenericConsumerProcessor = (job: unknown) => Promise<unknown>;

export abstract class BullWorkQueuesPort {
  abstract addJob(queueName: string, job: unknown, delay?: number): Promise<void>;
  abstract refreshQueuesConfig(fromDatabase?: boolean): Promise<void>;
  abstract addConsumerProcessor(
    queuePrefix: QueuePrefixes,
    processor: GenericConsumerProcessor,
  ): void;
  abstract addQueueConfig(queueConfig: BullWorkQueueConfig): Promise<void>;
  abstract editQueueConfig(queueConfig: BullWorkQueueConfig): Promise<void>;
  abstract deleteQueueConfig(queueName: string): Promise<void>;
  abstract createConsumer(queueName: string): void;
  abstract deleteConsumer(queueName: string): Promise<void>;
}
