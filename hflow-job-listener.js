#!/usr/bin/env node
/* -------------------------------------------------------------------------- *
 * Generic AMQP listener → spawns HyperFlow job executor (handleJob)          *
 * Async/await version                                                        *
 * -------------------------------------------------------------------------- */

const tracer =
    process.env.HF_VAR_ENABLE_TRACING === '1'
        ? require('./tracing.js')('serverless-task-executor')
        : undefined;

const amqplib  = require('amqplib');
const redis    = require('redis');
const { v4: uuidv4 } = require('uuid');
const handleJob      = require('./handler').handleJob;

/* ------------------------ runtime configuration --------------------------- */

const QUEUE_NAME = process.env.QUEUE_NAME;
if (!QUEUE_NAME) {
    console.error('QUEUE_NAME env is required');
    process.exit(1);
}

const AMQP_URL  = `amqp://${process.env.RABBIT_HOSTNAME || 'localhost'}`;
const AMQP_OPTS = {
    frameMax : parseInt(process.env.RABBIT_FRAME_MAX, 10) || 131_072,
    heartbeat: parseInt(process.env.RABBIT_HEARTBEAT, 10) || 60,
};

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const PREFETCH  = parseInt(process.env.RABBIT_PREFETCH_SIZE, 10) || 1;
const CONSUMER_TAG = uuidv4();

/* ------------------------ connection singletons -------------------------- */

let amqpConn   = null;
let amqpChan   = null;
let rcl  = null;
let msgInFlight = false;
let consumerCancelled = false;

/* ----------------------------- main logic -------------------------------- */

async function initAmqp() {
    if (amqpConn) return;
    console.log('[AMQP] Connecting →', AMQP_URL, 'frameMax=', AMQP_OPTS.frameMax);
    amqpConn = await amqplib.connect(AMQP_URL, AMQP_OPTS);

    amqpChan = await amqpConn.createChannel();
    await amqpChan.prefetch(PREFETCH);

    /* passive assert → fails if queue missing */
    await amqpChan.checkQueue(QUEUE_NAME);

    console.log('[AMQP] Waiting for messages on', QUEUE_NAME);
    await amqpChan.consume(
        QUEUE_NAME,
        onMessage,
        { noAck: false, consumerTag: CONSUMER_TAG }
    );
}

async function initRedis() {
    if (rcl) return;
    rcl = redis.createClient({ url: REDIS_URL });
    rcl.on('error', (e) => console.error('[Redis] error', e));
    await rcl.on('ready', () => console.log('[Redis] ready'));
}

/** handle single AMQP message */
async function onMessage(msg) {
    msgInFlight = true;
    console.log('[DEBUG] got msg', msg.content.toString())
    // const payload = JSON.parse(msg.content.toString());

    const taskId = msg.content.toString()

    try {
        console.time('handleJob');
        await executeTasks([taskId]); 
        console.timeEnd('handleJob');  
        amqpChan.ack(msg);
    } catch (err) {
        console.error('[Listener] task error', err);
        amqpChan.nack(msg);
    } finally {
        msgInFlight = false;
        if (consumerCancelled) maybeCloseAndExit();
    }
}

async function executeTasks(tasks) {
    for (const task of tasks) {
        let exitCode;
        try {
            exitCode = await handleJob(task, rcl, null);
        } catch (err) {
            console.error('[Listener] Executor error', err);
        console.log(`[Listener] task ${task} finished →`, exitCode);
        }
    }
}

/* --------------------------- graceful stop ------------------------------- */

process.on('SIGTERM', async () => {
    console.log('[Listener] SIGTERM received');
    consumerCancelled = true;
    if (amqpChan) await amqpChan.cancel(CONSUMER_TAG);
    if (!msgInFlight) maybeCloseAndExit();
});

async function maybeCloseAndExit() {
    console.log('[Listener] Shutting down');

    if (amqpChan)  await amqpChan.close();
    if (amqpConn)  await amqpConn.close();
    if (rcl) await rcl.quit();

    console.log('[Listener] Bye.');
    process.exit(0);
}

/* ------------------------------- start ----------------------------------- */

(async () => {
    try {
        await initRedis();
        await initAmqp();
    } catch (err) {
        console.error('[Startup] failed:', err);
        process.exit(1);
    }
})();
