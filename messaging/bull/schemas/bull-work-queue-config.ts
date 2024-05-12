// cria uma collection para guardar as configurações das filas bull que serão utilizadas na aplicação
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';
import { QueuePrefixes } from '../enums/queue-prefixes.enum';

export type BullWorkQueueConfigDocument = HydratedDocument<BullWorkQueueConfig>;

@Schema({ collection: 'bullworkqueueconfigs' })
export class BullWorkQueueConfig {
  @Prop({ required: true })
  name: string;

  // identificador do grupo de filas
  // cada grupo de filas é responsável por um contexto de negócio
  // cada contexto de negócio pode ter várias filas
  // o consumidor de cada fila é responsável por chamar o método processador correto de acordo com o contexto de negócio
  // isso é feito através do prefixo da fila e tratado no método addConsumerProcessor do BullWorkQueuesPort
  // internamente o BullWorkQueuesPort faz o mapeamento do prefixo da fila para o método processador correto
  @Prop({ required: true, enum: QueuePrefixes })
  queueGroupIndentity: QueuePrefixes;

  @Prop({ required: true, type: Object })
  workerLimiter: {
    max: number;
    duration: number;
    concurrency: number;
    stalledInterval: number;
    maxStalledCount: number; // should be 0
  };

  @Prop({ required: true, type: Object })
  jobOptions: {
    delay: number;
    attempts: number;
    backoff: number;
    removeOnComplete: boolean;
    removeOnFail: boolean;
  };
}

export const BullWorkQueueConfigSchema = SchemaFactory.createForClass(BullWorkQueueConfig);
