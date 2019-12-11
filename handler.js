#!/usr/bin/env node
// Executor of 'jobs' using the Redis task status notification mechanism

const { spawn } = require('child_process')
const redis = require('redis')
const fs=require('fs')
const log4js = require('log4js')
const pidtree = require('pidtree')

const {
    procfs,
    ProcfsError,
} = require('@stroncium/procfs');
 
if (process.argv.length < 4) {
  console.error("Usage: node handler.js <taskId> <redis_url>");
  process.exit(1);
}

// 'taskId' is the name of the Redis key (list) to use for the notification
var taskId = process.argv[2],
    redis_url = process.argv[3];

//console.log("taskId", taskId);
//console.log("redis_url", redis_url);

// check if working directory is set
if (process.env.HF_VAR_WORK_DIR) {
    process.chdir(process.env.HF_VAR_WORK_DIR);
} else if (fs.existsSync("/work_dir")) {
    process.chdir("/work_dir");
}

if (!fs.existsSync('logs-hf')) {
    fs.mkdirSync('logs-hf');
}

const loglevel = process.env.HF_VAR_LOG_LEVEL || 'info';
const logfilename = 'logs-hf/task-' + taskId.replace(/:/g, '__') + '.log';

log4js.configure({
    appenders: { hftrace: { type: 'file', filename: logfilename} },
    categories: { default: { appenders: ['hftrace'], level: loglevel } }
});

const logger = log4js.getLogger('hftrace');

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

let jobStart, jobEnd;

var pids = {}

// logging basic process info from the procfs
logProcInfo = function (pid) {
    // log process command line
    try {
        console.log("pid:", pid, "command:", JSON.stringify(procfs.processCmdline(pid)));
    } catch (error) {
        if (error.code === ProcfsError.ERR_NOT_FOUND) {
            console.error(`process ${pid} does not exist`);
        }
    }

    // periodically log process IO
    logProcIO = function (pid) {
        try {
            console.log("pid:", pid, "IO:", JSON.stringify(procfs.processIo(pid)));
            setTimeout(() => logProcIO(pid), 1000);
        } catch (error) {
            if (error.code === ProcfsError.ERR_NOT_FOUND) {
                console.error(`process ${pid} does not exist`);
            }
        }
    }
    logProcIO(pid)
}

async function executeJob() {

    logger.info('handler started');

    // 1. Get job message
    try {
        var jobMessage = await getJobMessage(0);
    } catch (err) {
        console.error(err);
        throw err;
    }
    logger.info('jobMessage: ', jobMessage)
    console.log("Received job message:", jobMessage);

    // 2. Execute job
    var jm = JSON.parse(jobMessage[1]);

    var stdoutStream;

    const cmd = spawn(jm["executable"], jm["args"]);
    let targetPid = cmd.pid;

    logProcInfo(targetPid)
    logger.info('job started')

    //console.log(Date.now(), 'job started');

    // make sure info about all child processes is logged (checks periodically for new pids)
    var allpids = {}
    addPidTree = function (pid) {
        pidtree(targetPid, function (err, pids) {
            //console.log(pids)
            if (!pids) return
            pids.map(p => {
                if (!allpids[p]) {
                    allpids[p] = "ok"
                    logProcInfo(p)
                }
            })
            setTimeout(() => addPidTree(pid), 1000);
        })
    }
    addPidTree(targetPid)

    // redirect process' stdout to a file
    if (jm["stdout"]) {
        stdoutStream = fs.createWriteStream(jm["stdout"], {flags: 'w'});
        cmd.stdout.pipe(stdoutStream);
    }

    cmd.stdout.on('data', (data) => {
      console.log(`stdout: ${data}`);
    });

    cmd.stderr.on('data', (data) => {
      console.error(`stderr: ${data}`);
    });

    cmd.on('close', async(code) => {
      //console.log(`child process exited with code ${code}`);
      // 3. Notify job completion
      try {
          await notifyJobCompletion();
          logger.info('job ended');
          //console.log(Date.now(), 'job ended');
      } catch (err) {
          console.error("Redis notification failed", err);
          logger.error("Redis notification failed: " + err)
          throw err;
      }
      logger.info('handler exiting');
      log4js.shutdown(function () { process.exit(0); })
    });
}

executeJob()