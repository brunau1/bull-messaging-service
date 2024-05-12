import { BullWorkQueuesPort } from 'src/core/ports/bull-work-queues.port';
import { Body, Controller, Delete, Get, Post, Put, Req, Request } from '@nestjs/common';
import { BullQueueConfigPayloadDto } from './dto/bull-work-queue.dto';

@Controller('messaging')
export class BullWorkQueuesController {
  constructor(private readonly bullWorkQueuesPort: BullWorkQueuesPort) {}

  @Get(['queues/refresh'])
  async refreshQueues(@Req() request: Request) {
    const fromDatabase = request.headers['refresh-from-db'] === 'true' ? true : false;

    return await this.bullWorkQueuesPort.refreshQueuesConfig(fromDatabase);
  }

  @Post(['queues/create'])
  async createQueue(@Body() payload: BullQueueConfigPayloadDto) {
    return await this.bullWorkQueuesPort.addQueueConfig(payload);
  }

  @Delete(['queues/delete/:queueName'])
  async deleteQueue(@Req() request: Request) {
    const queueName = request.url.split('/').pop();

    return await this.bullWorkQueuesPort.deleteQueueConfig(queueName);
  }

  @Put(['queues/edit'])
  async editQueue(@Body() payload: BullQueueConfigPayloadDto) {
    return await this.bullWorkQueuesPort.editQueueConfig(payload);
  }

  @Post(['consumers/add'])
  async addConsumer(@Body() payload: { queueName: string }) {
    return await this.bullWorkQueuesPort.createConsumer(payload.queueName);
  }

  @Delete(['consumers/delete/:queueName'])
  async deleteConsumer(@Req() request: Request) {
    const queueName = request.url.split('/').pop();

    return await this.bullWorkQueuesPort.deleteConsumer(queueName);
  }
}
