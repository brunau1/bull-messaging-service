import { Inject, Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';

import { Queue, Worker } from 'bullmq';
import { Model } from 'mongoose';

import { BullWorkQueuesPort, GenericConsumerProcessor } from 'src/core/ports/bull-work-queues.port';
import { StorageServicePort } from 'src/core/ports/storage-service.port';
import { EnvService } from 'src/infrastructure/env/env.service';
import { PixerLoggerService } from 'src/pixer-logger/pixer-logger.service';
import { Log } from 'src/utils/log/log';

import { DefaultQueueConfig } from './enums/default-queue.config';
import { QueuePrefixes } from './enums/queue-prefixes.enum';
import { ConsumerNotDefinedException } from './exceptions/consumer-not-defined.exception';
import { QueueConfigurationNotFoundException } from './exceptions/queue-configuration-not-found.exception';
import { QueueNotFoundException } from './exceptions/queue-not-fount.exception';
import { UndefinedProcessorException } from './exceptions/undefined-processor.exception';
import { BullWorkQueueConfig } from './schemas/bull-work-queue-config';

@Injectable()
export class BullWorkQueuesAdapter implements BullWorkQueuesPort {
  // as filas e os consumidores compartilham a mesma conexão com o redis
  // obs: se a fila não existir, o bull cria automaticamente
  private redisConfig = {
    host: EnvService.getEnv('REDIS_HOST'),
    port: parseInt(EnvService.getEnv('REDIS_PORT')),
  };

  private readonly ENABLE_QUEUE_CONSUMERS =
    EnvService.getEnv('ENABLE_QUEUE_CONSUMERS') === 'true' ? true : false;

  private readonly CONFIG_CACHE_KEY = 'bullworkqueueconfigs';

  private queues: Queue[]; // filas internas a partir do banco
  private defaultQueues: Queue[]; // filas padrão
  // consumidores das filas
  private consumers: {
    queueName: string;
    processor: GenericConsumerProcessor;
    worker: Worker; // consumidor da fila
  }[];
  private defaultConsumers: {
    queueName: string;
    processor: GenericConsumerProcessor;
    worker: Worker; // consumidor da fila
  }[];
  // processadores dos consumidores
  private consumerProcessors: { [key: string]: GenericConsumerProcessor };
  // configurações locais das filas
  private queuesConfig: BullWorkQueueConfig[];
  private defaultQueuesConfig: BullWorkQueueConfig[];

  constructor(
    @InjectModel(BullWorkQueueConfig.name)
    private readonly bullWorkQueueConfigModel: Model<BullWorkQueueConfig>,
    @Inject(PixerLoggerService)
    private readonly loggerService: PixerLoggerService,
    private readonly redisProvider: StorageServicePort,
  ) {
    this.queues = [];
    this.defaultQueues = [];

    this.consumers = [];
    this.defaultConsumers = [];

    this.queuesConfig = [];
    this.defaultQueuesConfig = [];

    this.consumerProcessors = {};
  }

  async onModuleInit() {
    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Bull queues service initializing...',
      }),
      this.constructor.name,
    );
    this.initDefaultQueues();

    await this.loadQueuesConfig();
    this.initQueues();

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Bull queues service initialized',
        payload: {
          activeQueues: [
            ...this.queues.map((queue) => queue.name),
            ...this.defaultQueues.map((queue) => queue.name),
          ],
        },
      }),
      this.constructor.name,
    );

    setTimeout(() => {
      // aguarda o carregamento das configurações das filas e
      // o preenchimento do array de processadores para criar os consumidores
      this.initDefaultConsumers();
      this.initConsumers();
    }, 5000);
  }

  private initQueues() {
    this.queues = [];

    if (!this.ENABLE_QUEUE_CONSUMERS) {
      this.loggerService.warnWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Dynamic queues were disabled by environment variable',
        }),
        this.constructor.name,
      );
      return;
    }

    this.queuesConfig.forEach((config) => {
      this.createQueue(config.name);
    });
  }

  private initDefaultQueues() {
    this.defaultQueues = [];
    this.defaultQueuesConfig = [];

    if (!this.ENABLE_QUEUE_CONSUMERS) {
      this.loggerService.warnWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Default queues were disabled by environment variable',
        }),
        this.constructor.name,
      );
      return;
    }

    Object.values(QueuePrefixes).forEach((prefix) => {
      const name = `${prefix}-default-queue`;
      this.createDefaultQueue(name);

      this.defaultQueuesConfig.push({
        name,
        queueGroupIndentity: prefix,
        workerLimiter: DefaultQueueConfig.workerLimiter,
        jobOptions: DefaultQueueConfig.jobOptions,
      });
    });
  }

  private initConsumers() {
    this.consumers = [];

    if (!this.ENABLE_QUEUE_CONSUMERS) {
      this.loggerService.warnWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Dynamic bull consumers were disabled by environment variable',
        }),
        this.constructor.name,
      );
      return;
    }

    for (const queueConfig of this.queuesConfig) {
      this.createConsumer(queueConfig.name);
    }
  }

  private initDefaultConsumers() {
    this.defaultConsumers = [];

    if (!this.ENABLE_QUEUE_CONSUMERS) {
      this.loggerService.warnWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Default bull consumers were disabled by environment variable',
        }),
        this.constructor.name,
      );
      return;
    }

    for (const queue of this.defaultQueues) {
      this.createConsumer(queue.name, true);
    }
  }

  private async loadQueuesConfig() {
    this.queuesConfig = this.queuesConfig || [];

    // Carrega as configurações padrão das filas a partir do banco de dados
    // popula o array de configurações das filas e o cache de configurações
    const queuesConfig = await this.bullWorkQueueConfigModel.find();

    if (queuesConfig.length === 0) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'No queues config found in database',
        }),
        this.constructor.name,
      );
      return;
    }

    this.queuesConfig = queuesConfig.map((config) => config.toObject());

    await this.redisProvider.setValue(this.CONFIG_CACHE_KEY, JSON.stringify(this.queuesConfig));

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Bull queues config loaded from database',
        payload: {
          queuesConfig: this.queuesConfig,
        },
      }),
      this.constructor.name,
    );
  }

  async refreshQueuesConfig(fromCache?: boolean) {
    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Refreshing queues config...',
      }),
      this.constructor.name,
    );

    if (!fromCache) {
      await this.loadQueuesConfig();
      this.initQueues();

      // apaga os consumidores atuais e cria novos
      await this.deleteAllConsumers();

      this.initConsumers();
    } else {
      // verificar necessidade de usar o cache
      await this.loadQueueConfigsFromCache();
      this.initQueues();

      // apaga os consumidores atuais e cria novos
      await this.deleteAllConsumers();

      this.initConsumers();
    }

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Queues config refreshed',
        payload: {
          activeQueues: this.queues.map((queue) => queue.name),
        },
      }),
      this.constructor.name,
    );
  }

  private async loadQueueConfigsFromCache() {
    const cacheConfigsJson = await this.redisProvider.getValueByKey(this.CONFIG_CACHE_KEY);
    const cacheConfigs: BullWorkQueueConfig[] = JSON.parse(cacheConfigsJson.toString());

    if (!cacheConfigs || cacheConfigs.length === 0) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'No queues config found in cache',
        }),
        this.constructor.name,
      );
      return;
    }

    this.queuesConfig = cacheConfigs;

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Bull queues config loaded from cache',
        payload: {
          queuesConfig: this.queuesConfig,
        },
      }),
      this.constructor.name,
    );
  }

  addConsumerProcessor(queuePrefix: QueuePrefixes, processor: GenericConsumerProcessor) {
    this.consumerProcessors[queuePrefix] = processor;

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Consumer processor added',
        payload: {
          queuePrefix,
          processorName: processor.name,
        },
      }),
      this.constructor.name,
    );
  }

  private getQueue(queueName: string, isDefaultQueue?: boolean) {
    const queue = isDefaultQueue
      ? this.defaultQueues.find((queue) => queue.name === queueName)
      : this.queues.find((queue) => queue.name === queueName);

    if (!queue) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Queue instance not found on application',
          payload: {
            queueName,
          },
        }),
        this.constructor.name,
      );
      throw new QueueNotFoundException(queueName);
    }
    return queue;
  }

  private async closeQueue(queueName: string) {
    const queue = this.getQueue(queueName);

    await queue.close();

    this.queues = this.queues.filter((queue) => queue.name !== queueName);
    this.queuesConfig = this.queuesConfig.filter((config) => config.name !== queueName);
  }

  private getQueueConfig(queueName: string, isDefaultQueue?: boolean) {
    const queueConfig = isDefaultQueue
      ? this.defaultQueuesConfig.find((config) => config.name === queueName)
      : this.queuesConfig.find((config) => config.name === queueName);

    if (!queueConfig) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Queue configuration not found',
          payload: {
            queueName,
          },
        }),
        this.constructor.name,
      );
      throw new QueueConfigurationNotFoundException(queueName);
    }
    return queueConfig;
  }

  private getQueueConfigByPrefix(queuePrefix: QueuePrefixes, isDefaultQueue?: boolean) {
    const queueConfig = isDefaultQueue
      ? this.defaultQueuesConfig.find((config) => config.queueGroupIndentity === queuePrefix)
      : this.queuesConfig.find((config) => config.queueGroupIndentity === queuePrefix);

    if (!queueConfig) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Queue configuration not found',
          payload: {
            queuePrefix,
          },
        }),
        this.constructor.name,
      );
      throw new QueueConfigurationNotFoundException(queuePrefix);
    }
    return queueConfig;
  }

  private checkProcessorRegistered(queuePrefix: QueuePrefixes) {
    if (!this.consumerProcessors[queuePrefix]) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Consumer processor not found',
          payload: {
            queuePrefix,
          },
        }),
        this.constructor.name,
      );
      throw new UndefinedProcessorException(queuePrefix);
    }
  }

  private createQueue(queueName: string): void {
    const queue = new Queue(queueName, {
      connection: this.redisConfig,
    });

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Queue created',
        payload: {
          queueName,
        },
      }),
      this.constructor.name,
    );

    this.queues.push(queue);
  }

  private createDefaultQueue(queueName: string): void {
    const queue = new Queue(queueName, {
      connection: this.redisConfig,
    });

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Default queue created',
        payload: {
          queueName,
        },
      }),
      this.constructor.name,
    );

    this.defaultQueues.push(queue);
  }

  async addQueueConfig(queueConfig: BullWorkQueueConfig): Promise<void> {
    // check first if the processor is already registered
    this.checkProcessorRegistered(queueConfig.queueGroupIndentity);

    if (this.queuesConfig.find((config) => config.name === queueConfig.name)) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Queue configuration already exists',
          payload: {
            queueName: queueConfig.name,
          },
        }),
        this.constructor.name,
      );
      return;
    }

    await this.bullWorkQueueConfigModel.create(queueConfig);

    this.queuesConfig.push({
      ...queueConfig,
    });

    await this.redisProvider.setValue(this.CONFIG_CACHE_KEY, JSON.stringify(this.queuesConfig));

    // cria a instância da fila
    this.createQueue(queueConfig.name);

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Queue configuration added',
        payload: {
          queueConfig,
        },
      }),
      this.constructor.name,
    );

    // cria o consumidor da fila
    this.createConsumer(queueConfig.name);
  }

  async editQueueConfig(queueConfig: BullWorkQueueConfig): Promise<void> {
    // Atualiza a configuração da fila
    // obs: não é necessário atualizar a fila, apenas a config, pois a conexão com o redis é a mesma
    await this.bullWorkQueueConfigModel.updateOne(
      {
        name: queueConfig.name,
      },
      {
        ...queueConfig,
      },
    );
    this.queuesConfig = this.queuesConfig.map((config) => {
      if (config.name === queueConfig.name) {
        return {
          ...queueConfig,
        };
      }
      return config;
    });

    await this.redisProvider.setValue(this.CONFIG_CACHE_KEY, JSON.stringify(this.queuesConfig));

    // Apaga o consumidor e cria um novo com o mesmo nome e processador, mas com a nova configuração
    await this.editConsumer(queueConfig.name);
  }

  async deleteQueueConfig(queueName: string): Promise<void> {
    // Apaga a configuração da fila do banco de dados, do cache e da memoria
    await this.bullWorkQueueConfigModel.deleteOne({
      name: queueName,
    });

    this.queues = this.queues.filter((queue) => queue.name !== queueName);
    this.queuesConfig = this.queuesConfig.filter((config) => config.name !== queueName);

    await this.redisProvider.setValue(this.CONFIG_CACHE_KEY, JSON.stringify(this.queuesConfig));

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Queue configuration removed',
        payload: {
          queueName,
        },
      }),
      this.constructor.name,
    );

    // Apaga o consumidor da fila
    await this.deleteConsumer(queueName);
  }

  // chamado na inicialização da aplicação ou na criação de um novo consumidor em tempo de execução
  // na edição das configurações da fila, o consumidor é apagado e criado novamente
  // é necessario que o metodo processador esteja registrado no objeto consumerProcessors
  // e que a fila esteja registrada no objeto queuesConfig
  createConsumer(queueName: string, isDefaultQueue?: boolean): void {
    if (!this.ENABLE_QUEUE_CONSUMERS) {
      this.loggerService.warnWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Bull queue consumers were disabled by environment variable',
        }),
        this.constructor.name,
      );
      return;
    }

    try {
      // check first if the processor is already registered
      const queueConfig = this.getQueueConfig(queueName, isDefaultQueue);

      this.checkProcessorRegistered(queueConfig.queueGroupIndentity);
      const processor = this.consumerProcessors[queueConfig.queueGroupIndentity];

      const worker = new Worker(queueName, processor, {
        connection: this.redisConfig,
        limiter: {
          max: queueConfig.workerLimiter.max,
          duration: queueConfig.workerLimiter.duration,
        },
        concurrency: queueConfig.workerLimiter.concurrency,
        stalledInterval: queueConfig.workerLimiter.stalledInterval,
        maxStalledCount: queueConfig.workerLimiter.maxStalledCount,
        removeOnComplete: {
          count: queueConfig.workerLimiter.completedJobsToKeep,
        },
        removeOnFail: {
          count: queueConfig.workerLimiter.failedJobsToKeep,
        },
      });

      const consumer = {
        queueName,
        processor,
        worker,
      };

      isDefaultQueue ? this.defaultConsumers.push(consumer) : this.consumers.push(consumer);

      this.loggerService.logWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Consumer created',
          payload: {
            queueName,
          },
        }),
        this.constructor.name,
      );
    } catch (error) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Error creating consumer',
          payload: {
            queueName,
            error,
          },
        }),
        this.constructor.name,
      );
    }
  }

  // o metodo deve ser pubçico para caso seja necessário apagar o consumidor em tempo de execução sem precisar apagar a fila
  async deleteConsumer(queueName: string, shouldCloseQueue?: boolean): Promise<void> {
    const consumerIndex = this.consumers.findIndex((consumer) => consumer.queueName === queueName);

    if (consumerIndex === -1) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Consumer not found',
          payload: {
            queueName,
          },
        }),
        this.constructor.name,
      );
      throw new ConsumerNotDefinedException(queueName);
    }

    // aguarda a finalização dos jobs em execução e fecha o consumidor
    await this.consumers[consumerIndex].worker.close();
    // apaga o consumidor da lista
    this.consumers = this.consumers.filter((consumer) => consumer.queueName !== queueName);

    if (shouldCloseQueue) {
      // fecha a fila
      await this.closeQueue(queueName);
    }

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Consumer deleted',
        payload: {
          queueName,
        },
      }),
      this.constructor.name,
    );
  }

  // Apaga o consumidor atual e cria um novo com o mesmo nome e processador
  private async editConsumer(queueName: string): Promise<void> {
    await this.deleteConsumer(queueName);

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Old consumer deleted. Creating new consumer with new configuration',
        payload: {
          queueName,
        },
      }),
      this.constructor.name,
    );

    this.createConsumer(queueName);
  }

  private async deleteAllConsumers() {
    await Promise.all(
      this.consumers.map((consumer) => this.deleteConsumer(consumer.queueName, true)),
    );

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'All consumers deleted',
      }),
      this.constructor.name,
    );
  }

  async addJob(
    queueName: string,
    job: unknown,
    jobOptions?: { delay?: number; queueIdentity: QueuePrefixes },
  ): Promise<void> {
    let queue: Queue;
    let queueConfig: BullWorkQueueConfig;

    try {
      queue = this.getQueue(queueName);
      queueConfig = this.getQueueConfig(queueName);
    } catch (error) {
      this.loggerService.warnWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Error on finding the queue. Trying add to default queue',
          payload: {
            queueName,
            queueIdentity: jobOptions.queueIdentity,
            error: error.message,
          },
        }),
        this.constructor.name,
      );

      queueConfig = this.getQueueConfigByPrefix(jobOptions.queueIdentity, true);
      queue = this.getQueue(queueConfig.name, true);
    }

    await queue.add(queue.name, job, {
      attempts: queueConfig.jobOptions.attempts,
      backoff: queueConfig.jobOptions.backoff,
      delay: jobOptions?.delay || queueConfig.jobOptions.delay,
      removeOnComplete: queueConfig.jobOptions.removeOnComplete,
      removeOnFail: queueConfig.jobOptions.removeOnFail,
    });

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: `Job added to queue ${queue.name}`,
        payload: {
          queueName: queue.name,
          job,
        },
      }),
      this.constructor.name,
    );
  }
}
