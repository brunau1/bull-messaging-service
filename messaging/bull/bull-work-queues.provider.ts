import { Provider } from '@nestjs/common';
import { BullWorkQueuesPort } from 'src/core/ports/bull-work-queues.port';
import { BullWorkQueuesAdapter } from './bull-work-queues.adapter';

export const BullWorkQueuesProvider: Provider<BullWorkQueuesPort> = {
  provide: BullWorkQueuesPort,
  useClass: BullWorkQueuesAdapter,
};
