import { IsNotEmpty } from 'class-validator';
import { QueuePrefixes } from '../enums/queue-prefixes.enum';

export class BullQueueConfigPayloadDto {
  @IsNotEmpty()
  name: string;

  @IsNotEmpty()
  queueGroupIndentity: QueuePrefixes;

  @IsNotEmpty()
  workerLimiter: {
    max: number;
    duration: number;
    concurrency: number;
    stalledInterval: number;
    maxStalledCount: number; // should be 0
  };

  @IsNotEmpty()
  jobOptions: {
    delay: number;
    attempts: number;
    backoff: number;
    removeOnComplete: boolean;
    removeOnFail: boolean;
  };
}
