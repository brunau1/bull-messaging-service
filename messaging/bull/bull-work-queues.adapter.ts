import { Inject, Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';

import { Queue, Worker } from 'bullmq';
import { Model } from 'mongoose';

import { BullWorkQueuesPort, GenericConsumerProcessor } from 'src/core/ports/bull-work-queues.port';
import { StorageServicePort } from 'src/core/ports/storage-service.port';
import { EnvService } from 'src/infrastructure/env/env.service';

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
    host: EnvService.getEnv('REDIS_HOST').getValue(),
    port: parseInt(EnvService.getEnv('REDIS_PORT').getValue()),
  };

  private readonly ENABLE_QUEUE_CONSUMERS =
    EnvService.getEnv('ENABLE_QUEUE_CONSUMERS').getValue() === 'true' ? true : false;
  // deve ser habilitado caso as filas da instancia atual devam emitir eventos de controle
  private readonly ENABLE_QUEUE_EVENTS =
    EnvService.getEnv('ENABLE_QUEUE_EVENTS').getValue() === 'true' ? true : false;

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
  private consumerProcessors: { [key: QueuePrefixes | string]: GenericConsumerProcessor };
  // controlador de eventos das filas
  private queueGroupEvents: {
    [key: QueuePrefixes | string]: {
      queues: { [key: string]: QueueEvents };
      defaultQueues: { [key: string]: QueueEvents };
      successCallback: GenericQueueEventHandler;
      failureCallback: GenericQueueEventHandler;
    };
  };
  // configurações locais das filas
  private queuesConfig: BullWorkQueueConfig[];
  private defaultQueuesConfig: BullWorkQueueConfig[];

  constructor(
    @InjectModel(BullWorkQueueConfig.name)
    private readonly bullWorkQueueConfigModel: Model<BullWorkQueueConfig>,
    private readonly loggerService: LoggerService,
    private readonly redisProvider: StorageServicePort,
    private readonly configurationProvider: ConfigurationPort,
  ) {
    this.queues = [];
    this.defaultQueues = [];

    this.consumers = [];
    this.defaultConsumers = [];

    this.queuesConfig = [];
    this.defaultQueuesConfig = [];

    this.consumerProcessors = {};
    this.queueGroupEvents = {};
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
          message:
            'Dynamic queues were initialized but consumers were disabled by environment variable',
        }),
        this.constructor.name,
      );
    }

    this.queuesConfig.forEach((config) => {
      this.createQueue(config.name);
      // para casos onde não precisamos habilitar os eventos da fila
      // obs: os eventos devem ser habilitados por padrão para as rotinas da aplicação
      if (this.ENABLE_QUEUE_EVENTS) this.startQueueEventListeners(config.name, false);
    });
  }

  private initDefaultQueues() {
    this.defaultQueues = [];
    this.defaultQueuesConfig = [];

    if (!this.ENABLE_QUEUE_CONSUMERS) {
      this.loggerService.warnWithPayload(
        new Log({
          scope: this.constructor.name,
          message:
            'Default queues are initialized but consumers were disabled by environment variable',
        }),
        this.constructor.name,
      );
    }

    Object.values(QueuePrefixes).forEach((prefix) => {
      const name = this.generateDefaultQueueName(prefix);
      this.createDefaultQueue(name);

      this.defaultQueuesConfig.push({
        name,
        queueGroupIdentity: prefix,
        workerLimiter: DefaultQueueConfig.workerLimiter,
        jobOptions: DefaultQueueConfig.jobOptions,
      });

      if (this.ENABLE_QUEUE_EVENTS) this.startQueueEventListeners(name, true);
    });
  }

  private generateDefaultQueueName = (prefix: string) => `${prefix}-default-queue`;
  // generate a unique job id containning the queue prefix and a random string of 10 characters
  // must not contain special characters
  private generateJobId = (prefix: string) =>
    `${prefix}${Math.random().toString(36).slice(2, 12)}`.replace(/[^a-zA-Z0-9]/g, '');

  private normalizeJobId = (jobId: string) => (jobId ? jobId.replace(/[^a-zA-Z0-9]/g, '') : null);

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
      const queuesConsumersToIgnore = this.configurationProvider.getValue<string>(
        ConfigurationKey.queueCosumersToIgnore,
      );
      const listOfQueuesConsumersToIgnore = !!queuesConsumersToIgnore
        ? queuesConsumersToIgnore?.split(',')
        : [];

      if (
        !!listOfQueuesConsumersToIgnore.length &&
        listOfQueuesConsumersToIgnore.includes(queueConfig.name)
      ) {
        continue;
      }

      this.createConsumer(queueConfig.name, false);
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
      const queuesConsumersToIgnore = this.configurationProvider.getValue<string>(
        ConfigurationKey.queueCosumersToIgnore,
      );
      const listOfQueuesConsumersToIgnore = !!queuesConsumersToIgnore
        ? queuesConsumersToIgnore
            ?.split(',')
            .map((queueName: string) => this.generateDefaultQueueName(queueName))
        : [];

      if (
        !!listOfQueuesConsumersToIgnore.length &&
        listOfQueuesConsumersToIgnore.includes(queue.name)
      ) {
        continue;
      }

      this.createConsumer(queue.name, true);
    }
  }

  private async loadQueuesConfig() {
    this.queuesConfig = [];

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
      // apaga os consumidores atuais e cria novos
      await this.deleteAllConsumers();

      await this.loadQueuesConfig();
      this.initQueues();

      this.initConsumers();
    } else {
      // apaga os consumidores atuais e cria novos
      await this.deleteAllConsumers();

      // verificar necessidade de usar o cache
      await this.loadQueueConfigsFromCache();
      this.initQueues();

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
    const cacheConfigsJson = await this.redisProvider.getValueByKeyWithPrefix(
      this.CONFIG_CACHE_KEY,
    );
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

  // Adiciona um manipulador de eventos para a fila
  // Deve ser chamado na inicialização da aplicação da mesma forma que o método addConsumerProcessor
  // obs: o manipulador de eventos é chamado quando um job está completado ou com falha
  addQueueEventHandler(
    queuePrefix: QueuePrefixes,
    successCallback?: GenericQueueEventHandler,
    failureCallback?: GenericQueueEventHandler,
  ) {
    this.queueGroupEvents[queuePrefix] = {
      queues: null,
      defaultQueues: null,
      successCallback: successCallback || null,
      failureCallback: failureCallback || null,
    };

    this.loggerService.logWithPayload(
      new Log({
        scope: this.constructor.name,
        message: 'Queue event handler added',
        payload: {
          queuePrefix,
        },
      }),
      this.constructor.name,
    );
  }

  // Inicia os listeners de eventos da fila
  private startQueueEventListeners(queueName: string, isDefaultQueue?: boolean) {
    try {
      const queueConfig = this.getQueueConfig(queueName, isDefaultQueue);

      const hasConfig = !!this.queueGroupEvents[queueConfig.queueGroupIdentity];

      if (hasConfig) {
        const queueEvents = new QueueEvents(queueName, {
          connection: this.redisConfig,
          autorun: true,
        });

        const { successCallback, failureCallback } =
          this.queueGroupEvents[queueConfig.queueGroupIdentity];

        if (!!successCallback) {
          queueEvents.on(
            'completed',
            (async (data) => {
              try {
                await successCallback(data);
              } catch (error) {
                this.loggerService.errorWithPayload(
                  new Log({
                    scope: this.constructor.name,
                    message: 'Error executing queue success callback',
                    payload: {
                      queueName,
                      error,
                    },
                  }),
                  this.constructor.name,
                );
              }
            }).bind(this),
          );
        }

        if (!!failureCallback) {
          queueEvents.on(
            'failed',
            (async (data) => {
              try {
                await failureCallback(data);
              } catch (error) {
                this.loggerService.errorWithPayload(
                  new Log({
                    scope: this.constructor.name,
                    message: 'Error executing queue failure callback',
                    payload: {
                      queueName,
                      error,
                    },
                  }),
                  this.constructor.name,
                );
              }
            }).bind(this),
          );
        }

        const hasAtLeastOneCallback = !!successCallback || !!failureCallback;
        // salva a instância do listener para poder ser acessada posteriormente
        isDefaultQueue && hasAtLeastOneCallback
          ? (this.queueGroupEvents[queueConfig.queueGroupIdentity].defaultQueues[queueName] =
              queueEvents)
          : hasAtLeastOneCallback
          ? (this.queueGroupEvents[queueConfig.queueGroupIdentity].queues[queueName] = queueEvents)
          : null;
      } else {
        this.loggerService.logWithPayload(
          new Log({
            scope: this.constructor.name,
            message: 'Queue event listeners not set',
            payload: {
              queueName,
            },
          }),
          this.constructor.name,
        );
      }
    } catch (error) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Error starting queue event listeners',
          payload: {
            queueName,
            error,
          },
        }),
        this.constructor.name,
      );
    }
  }

  private getQueue(queueName: string, isDefaultQueue?: boolean) {
    const queue = isDefaultQueue
      ? this.defaultQueues.find((queue) => queue.name === queueName)
      : this.queues.find((queue) => queue.name === queueName);

    if (!queue) {
      throw new QueueNotFoundException(queueName);
    }
    return queue;
  }

  private hasActiveEvents(queueName: string, isDefaultQueue?: boolean) {
    const queueConfig = this.getQueueConfig(queueName, isDefaultQueue);
    const queueEvents = this.queueGroupEvents[queueConfig.queueGroupIdentity];

    if (!queueEvents) {
      return false;
    }

    if (isDefaultQueue) {
      return !!queueEvents.defaultQueues[queueName];
    }

    return !!queueEvents.queues[queueName];
  }

  private async closeQueue(queueName: string) {
    const queue = this.getQueue(queueName);
    const hasActiveEvents = this.hasActiveEvents(queueName);

    // fecha os listeners de eventos da fila caso existam
    if (hasActiveEvents) {
      const queueConfig = this.getQueueConfig(queueName);
      const queueEvents = this.queueGroupEvents[queueConfig.queueGroupIdentity];

      await queueEvents.queues[queueName].close();
      delete queueEvents.queues[queueName];
    }

    // aguarda a finalização dos jobs em execução
    await queue.drain();
    await queue.close();

    this.queues = this.queues.filter((queue) => queue.name !== queueName);
    this.queuesConfig = this.queuesConfig.filter((config) => config.name !== queueName);

    await this.redisProvider.setValue(this.CONFIG_CACHE_KEY, JSON.stringify(this.queuesConfig));
  }

  private getQueueConfig(queueName: string, isDefaultQueue?: boolean) {
    const queueConfig = isDefaultQueue
      ? this.defaultQueuesConfig.find((config) => config.name === queueName)
      : this.queuesConfig.find((config) => config.name === queueName);

    if (!queueConfig) {
      throw new QueueConfigurationNotFoundException(queueName);
    }

    return queueConfig;
  }

  private getQueueConfigByPrefix(queuePrefix: QueuePrefixes, isDefaultQueue?: boolean) {
    const queueConfig = isDefaultQueue
      ? this.defaultQueuesConfig.find((config) => config.queueGroupIdentity === queuePrefix)
      : this.queuesConfig.find((config) => config.queueGroupIdentity === queuePrefix);

    if (!queueConfig) {
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
    this.checkProcessorRegistered(queueConfig.queueGroupIdentity as QueuePrefixes);

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

    if (this.ENABLE_QUEUE_EVENTS) this.startQueueEventListeners(queueConfig.name, false);
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
    // Apaga o consumidor da fila
    await this.deleteConsumer(queueName);
    await this.closeQueue(queueName);

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

      this.checkProcessorRegistered(queueConfig.queueGroupIdentity as QueuePrefixes);
      const processor = this.consumerProcessors[queueConfig.queueGroupIdentity];

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

  private wasCreatedQueueInstanceAndConfig(queueName: string): boolean {
    try {
      this.getQueueConfig(queueName, false);
      this.getQueue(queueName, false);

      return true;
    } catch (error) {
      return false;
    }
  }

  async addJob(jobData: unknown, jobOptions: JobOptionsDto): Promise<string> {
    let queue: Queue;
    let queueConfig: BullWorkQueueConfig;

    try {
      const queueName = jobOptions.queueName;
      const queueExists = !!queueName ? this.wasCreatedQueueInstanceAndConfig(queueName) : false;

      if (!queueExists) {
        queueConfig = this.getQueueConfigByPrefix(jobOptions.queueIdentity, true);
        queue = this.getQueue(queueConfig.name, true);
      } else {
        queue = this.getQueue(jobOptions.queueName, false);
        queueConfig = this.getQueueConfig(jobOptions.queueName, false);
      }
      const jobId =
        this.normalizeJobId(jobOptions.jobId) || this.generateJobId(queueConfig.queueGroupIdentity);

      await queue.add(queue.name, jobData, {
        jobId,
        attempts: queueConfig.jobOptions.attempts,
        backoff: queueConfig.jobOptions.backoff,
        delay: jobOptions.delay || queueConfig.jobOptions.delay,
        removeOnComplete: queueConfig.jobOptions.removeOnComplete,
        removeOnFail: queueConfig.jobOptions.removeOnFail,
      });

      this.loggerService.logWithPayload(
        new Log({
          scope: this.constructor.name,
          message: `Job added to queue ${queue.name}`,
          payload: {
            jobOptions,
          },
        }),
        this.constructor.name,
      );

      return jobId;
    } catch (error) {
      this.loggerService.errorWithPayload(
        new Log({
          scope: this.constructor.name,
          message: 'Error adding job to queue',
          payload: {
            jobOptions,
            message: error.message,
            stack: error.stack,
          },
        }),
        this.constructor.name,
      );
    }
  }
}
