#!/usr/bin/env node
// Executor of 'jobs' using the Redis task status notification mechanism

const { spawn } = require('child_process')
const redis = require('redis')
const fs = require('fs')
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

// FIXME: race here with NFS
if (!fs.existsSync('logs-hf')) {
    fs.mkdirSync('logs-hf');
}

const loglevel = process.env.HF_VAR_LOG_LEVEL || 'info';
const logfilename = 'logs-hf/task-' + taskId.replace(/:/g, '__') + '.log';
const stdoutfilename = 'logs-hf/task-' + taskId.replace(/:/g, '__') + '__stdout.log';
const stderrfilename = 'logs-hf/task-' + taskId.replace(/:/g, '__') + '__stderr.log';
var stdoutLog = fs.createWriteStream(stdoutfilename, {flags: 'w'});
var stderrLog = fs.createWriteStream(stderrfilename, {flags: 'w'});
const enableNethogs = process.env.HF_VAR_ENABLE_NETHOGS;
const nethogsfilename = 'logs-hf/task-' + taskId.replace(/:/g, '__') + '__nethogs.log';

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
            err ? reject(err): resolve(reply);
        });
    });
}

// send notification about job completion to Redis
// 'code' is the job's exit code
var notifyJobCompletion = async function (code) {
    return new Promise(function (resolve, reject) {
        rcl.rpush(taskId, code, function (err, reply) {
            err ? reject(err): resolve(reply);
        });
    });
}

let jobStart, jobEnd;

var pids = {} // pids of the entire pid tree (in case the main process starts child processes)
var jm;  // parsed job message

// logging basic process info from the procfs
logProcInfo = function (pid) {
    // log process command line
    try {
        let cmdInfo = { "pid": pid, "name": jm["name"], "command": procfs.processCmdline(pid) };
        logger.info("command:", JSON.stringify(cmdInfo));
    } catch (error) {
        if (error.code === ProcfsError.ERR_NOT_FOUND) {
            console.error(`process ${pid} does not exist`);
        }
    }

    // periodically log process IO
    logProcIO = function (pid) {
        try {
            let ioInfo = procfs.processIo(pid);
            ioInfo.pid = pid;
            ioInfo.name = jm["name"];
            logger.info("IO:", JSON.stringify(ioInfo));
            setTimeout(() => logProcIO(pid), 2000);
        } catch (error) {
            if (error.code === ProcfsError.ERR_NOT_FOUND) {
                console.error(`process ${pid} does not exist (this is okay)`);
            }
        }
    }
    logProcIO(pid);

    logProcNetDev = function (pid) {
        try {
            let netDevInfo = procfs.processNetDev(pid);
            //netDevInfo.pid = pid;
            //netDevInfo.name = jm["name"];
            logger.info("NetDev: pid:", pid, JSON.stringify(netDevInfo));
            setTimeout(() => logProcNetDev(pid), 2000);
        } catch (error) {
            if (error.code === ProcfsError.ERR_NOT_FOUND) {
                //console.error(`process ${pid} does not exist (this is okay)`);
            }
        }
    }
    logProcNetDev(pid);
}

// enable network IO monitoring using nethogs 
if (enableNethogs) {
    var nethogsStream = fs.createWriteStream(nethogsfilename, {flags: 'w'});
    const nethogs = spawn("nethogs-wrapper.py");
    nethogs.stdout.pipe(nethogsStream);
    nethogs.on('error', function(err){ 
        logger.error("nethogs execution error:", err);
    });
}

var numRetries = process.env.HF_VAR_NUMBER_OF_RETRIES || 1;
var backoffSeed = process.env.HF_VAR_BACKOFF_SEED || 10;
async function executeJob(jm, attempt) {
    var stdoutStream;

    numRetries--;
    const cmd = spawn(jm["executable"], jm["args"]);
    let targetPid = cmd.pid;
    cmd.stdout.pipe(stdoutLog);
    cmd.stderr.pipe(stderrLog);

    logProcInfo(targetPid);
    logger.info('job started:', jm["name"]);

    //console.log(Date.now(), 'job started');

    // make sure info about all child processes is logged (checks periodically for new pids)
    var allpids = {}
    addPidTree = function (pid) {
        pidtree(targetPid, function (err, pids) {
            //console.log(pids)
            if (!pids) return;
            pids.map(p => {
                if (!allpids[p]) {
                    allpids[p] = "ok";
                    logProcInfo(p);
                }
            });
            setTimeout(() => addPidTree(pid), 1000);
        });
    }
    addPidTree(targetPid);

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
      if (code != 0) {
        logger.info("job failed (try " + attempt + "): '" + jm["executable"], jm["args"].join(' ') + "'");
      } else {
        logger.info('job successful (try ' + attempt + '):', jm["name"]);
      }
      logger.info('job exit code:', code);

      // retry the job
      if (code !=0 && numRetries > 0) {
        logger.info('Retrying job, number of retries left:', numRetries);
        cmd.removeAllListeners();
        // need to recreate write streams to log files for the retried job
        stdoutLog = fs.createWriteStream(stdoutfilename, {flags: 'a'});
        stderrLog = fs.createWriteStream(stderrfilename, {flags: 'a'});
        var factor = attempt > 5 ? 5: attempt;
        var backoffDelay = Math.floor(Math.random() * backoffSeed * factor); 
        logger.info('Backoff delay:', backoffDelay, 's');
        setTimeout(function() { executeJob(jm, attempt+1); }, backoffDelay*1000);
      } else {
        // Notify job completion to HyperFlow
        try {
            await notifyJobCompletion(code);
           //console.log(Date.now(), 'job ended');
        } catch (err) {
            console.error("Redis notification failed", err);
            logger.error("Redis notification failed: " + err);
            throw err;
        }
        logger.info('handler exiting');
        log4js.shutdown(function () { process.exit(code); });
      }
    });
}

async function waitForInputs(files, max_retries) {
    return new Promise((resolve, reject) => {
        var filesToWatch = files;
        var filesReady = [];
        var num_retries = 0;
    
        var checkFiles = function() {
            var filesChecked = 0;
            var nFilesLeft = filesToWatch.length;

            if (num_retries > max_retries) {
                logger.info("Error waiting for input files", files);
                return reject("Error waiting for input files", files);
            }

            //logger.info("Waiting for input files: (" + num_retries + ")", files);
            logger.info('waitingForFiles (' + num_retries + '): { "timestamp":', Date.now() +
                        ', "waitingForFiles":', JSON.stringify(filesToWatch) + ', "filesReady":', 
                        JSON.stringify(filesReady), "}");
                        
            num_retries++;
            
            var filesFoundIdx = [];
            filesToWatch.forEach((file, i) => {
                if (fs.existsSync(file)) {
                    filesChecked++;
                    filesReady.push({"file": file, "readTime": Date.now()});
                    filesFoundIdx.push(i);
                    delete filesToWatch[i];
                }
            });
            filesToWatch = filesToWatch.filter(f => { return f; });

            if (filesFoundIdx.length) {
                //filesToWatch.forEach((_, i) => filesToWatch.splice(i, 1));
                logger.info('filesReady (' + num_retries + '): { "timestamp":', Date.now() +
                            ', "waitingForFiles":', JSON.stringify(filesToWatch) + ', "filesReady":', 
                            JSON.stringify(filesReady), "}");
            }

            if (filesToWatch.length == 0) {
                logger.info("All input files ready!");
                return resolve();
            } else {
                const t = Math.pow(2, num_retries)+1000;
                setTimeout(() => {
                    checkFiles();
                }, t);
            }
        }

        checkFiles();
    });
}

async function handleJob() { 
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
    jm = JSON.parse(jobMessage[1]);

    // 2. Check/wait for input files
    if (jm.inputs && jm.inputs.length) {
        var files = jm.inputs.map(input => input.name).slice();
        try {
            await waitForInputs(files, process.env.HF_VAR_FILE_WATCH_NUM_RETRIES || 10);
        } catch(err) {
            throw err;
        }
    }

    // 3. Execute job
    logger.info("Job command: '" + jm["executable"], jm["args"].join(' ') + "'");
    executeJob(jm, 1);
}

handleJob();