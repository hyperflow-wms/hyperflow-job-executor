#!/usr/bin/env node
// Executor of 'jobs' using the Redis task status notification mechanism

const { spawn } = require('child_process');
const redis = require('redis');
const fs=require('fs');

if (process.argv.length < 4) {
  console.error("Usage: node handler.js <taskId> <redis_url>");
  process.exit(1);
}

// 'taskId' is the name of the Redis key (list) to use for the notification
var taskId = process.argv[2],
    redis_url = process.argv[3];

//console.log("taskId", taskId);
//console.log("redis_url", redis_url);

var rcl = redis.createClient(redis_url);

// get job message from Redis
var getJobMessage = async function (timeout) {
    return new Promise(function (resolve, reject) {
        const jobMsgKey = taskId + "_msg";
        rcl.brpop(jobMsgKey, timeout, function (err, reply) {
            err ? reject(err): resolve(reply)
        });
    });
}

// send notification about job completion to Redis
var notifyJobCompletion = async function () {
    return new Promise(function (resolve, reject) {
        rcl.rpush(taskId, "OK", function (err, reply) {
            err ? reject(err): resolve(reply)
        });
    });
}

async function executeJob() {

    // 1. Get job message
    try {
        var jobMessage = await getJobMessage(0);
    } catch (err) {
        console.error(err);
        throw err;
    }
    console.log("Received job message:", jobMessage);

    // 2. Execute job
    var jm = JSON.parse(jobMessage[1]);

    // check if working directory is set
    if (process.env.HF_VAR_WORK_DIR) {
        process.chdir(process.env.HF_VAR_WORK_DIR);
    } else if (fs.existsSync("/work_dir")) {
        process.chdir("/work_dir");
    }

    var stdoutStream;

    const cmd = spawn(jm["executable"], jm["args"]);

    // redirect process' stdout to a file
    if (config.executor.stdout) {
        stdoutStream = fs.createWriteStream(config.executor.stdout, {flags: 'w'});
        proc.stdout.pipe(stdoutStream);
    }

    cmd.stdout.on('data', (data) => {
      //console.log(`stdout: ${data}`);
    });

    cmd.stderr.on('data', (data) => {
      console.error(`stderr: ${data}`);
    });

    cmd.on('close', async(code) => {
      //console.log(`child process exited with code ${code}`);
      // 3. Notify job completion
      try {
          await notifyJobCompletion();
      } catch (err) {
          console.error("Redis notification failed", err);
          throw err;
      }
      process.exit(0);
    });
}

executeJob()
