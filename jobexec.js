#!/usr/bin/env node

// HyperFlow job executor 
// Usually takes two arguments: <taskId> and <redisUrl>
// Can also take a list of jobs (<taskId>...) in in which case 
// it will execute them *sequentially* (used for agglomeration of small jobs)

// The executor communicates via Redis as follows:
// '<taskId>_msg' is the Redis key where the job message is retrieved from
// '<taskId>' is the Redis key where job exit code is returned 

// Terminology: 
// 'task': a task to be executed within a workflow node
// 'job': a concrete execution of the task (a task could have multiple jobs/retries)
const tracer = process.env.HF_VAR_ENABLE_TRACING  === "1" ? require("./tracing.js")("hyperflow-job-executor"): undefined;
const otel = require('@opentelemetry/api')

const redis = require('redis');
var handleJob = require('./handler').handleJob;
var docopt = require('docopt').docopt;

var doc = "\
Usage:\n\
  hflow-job-execute <taskId> <redisUrl> <parentId> <traceId>\n\
  hflow-job-execute <redisUrl> <parentId> <traceId> -a [--] <taskId>...\n\
  hflow-job-execute -h | --help";

var opts = docopt(doc);
var tasks = opts['<taskId>'];
console.log("Job executor will execute tasks:", tasks.join(" "));
var redisUrl = opts['<redisUrl>'];
var parentId = opts['<parentId>'];
var traceId = opts['<traceId>'];
var rcl = redis.createClient(redisUrl);

// Execute tasks 
async function executeTask(idx) {
    if (idx < tasks.length) {
        let jobExitCode = await handleJob(tasks[idx], rcl, null);
        console.log("Task", tasks[idx], "job exit code:", jobExitCode);
        executeTask(idx+1);
    } else {
        // No more tasks to handle; stop redis client
        rcl.quit();
    }
}
if(process.env.HF_VAR_ENABLE_TRACING  === "0"){
    executeTask(0);
} else {
    const spanContext = {
        traceId: traceId,
        spanId: parentId,
        isRemote: true,
        traceFlags: otel.TraceFlags.SAMPLED
    }
    const context = otel.trace.setSpanContext(otel.context.active(), spanContext);

    otel.context.with(context, () => {
        tracer.startActiveSpan('job-executor', span => {
            executeTask(0);
            span.end();
        });
    })
}

