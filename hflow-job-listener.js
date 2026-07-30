#!/usr/bin/env node

const amqp = require('amqplib/callback_api'),
    redis = require('redis'),
    rcl = redis.createClient(process.env.REDIS_URL),
    uuid = require('uuid'),
    handleJob = require('./handler').handleJob,
    clog = require('./consoleLogger');

const queue = process.env.QUEUE_NAME;
const CONSUMER_TAG = uuid.v4();

let connection_handler = null
let channel_handler = null
let msg_processing = false
let consumer_created = false
let consumer_cancelled = false

process.on('SIGTERM', async () => {
    clog.info("SIGTERM received. Closing process")
    if (channel_handler !== null && consumer_created) {
        await channel_handler.cancel(CONSUMER_TAG);
    }
    consumer_cancelled = true

    if (msg_processing === false) {
        setTimeout(closeConnections, 5000);
    }
})

async function executeTask(tasks) {
    for (let idx = 0; idx < tasks.length; idx++) {
        let jobExitCode = await handleJob(tasks[idx].id, rcl, tasks[idx].message);
        clog.debug("Task", tasks[idx], "job exit code:", jobExitCode);
    }
}

async function onMessage(channel, msg) {
    clog.debug(" [x] Received %s", msg.content.toString());
    msg_processing = true
    executeTask(JSON.parse(msg.content).tasks).then((value) => {
        clog.debug("Message completed")
        channel.ack(msg)
        msg_processing = false
    }).catch(function () {
        clog.error("Message processing error")
        channel.nack(msg);
        msg_processing = false
    }).finally(function () {
        if (consumer_cancelled === true) {
            setTimeout(closeConnections, 5000);
        }
    });
}

function onChannelCreated(error, channel) {
    if (error) {
        throw error;
    }
    channel_handler = channel
    const consumerOptions = {noAck: false, consumerTag: CONSUMER_TAG}
    const queueOptions = {durable: false, expires: 6000000}
    const prefetch = parseInt(process.env['RABBIT_PREFETCH_SIZE']) || 1

    channel.prefetch(prefetch);
    channel.assertQueue(queue, queueOptions);

    clog.info(" [*] Waiting for messages in queue: %s", queue);
    clog.info("Consumer tag: " + CONSUMER_TAG);
    channel.consume(queue, (msg) => onMessage(channel, msg), consumerOptions);
    consumer_created = true
}

function onConnectionCreated(error, connection) {
    if (error) {
        throw error;
    }
    connection_handler = connection
    connection.createChannel(onChannelCreated);
}

async function closeConnections() {
    clog.debug("Terminate listener invoked")
    if (channel_handler !== null) {
        await channel_handler.close()
        clog.debug("RabbitMQ channel closed")
    }
    if (connection_handler !== null) {
        await connection_handler.close()
        clog.debug("RabbitMQ connection closed")
    }
    if (rcl !== null) {
        await rcl.quit()
        clog.debug("Redis connection closed")
    }
    clog.debug("Terminate listener processed")
    process.exit(0)
}

amqp.connect(`amqp://${process.env.RABBIT_HOSTNAME}`, onConnectionCreated);
