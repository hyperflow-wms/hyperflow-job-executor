#!/usr/bin/env node
// Executor of 'jobs' using the Redis task status notification mechanism

const tracer = process.env.HF_VAR_ENABLE_TRACING  === "1" ? require("./tracing.js")("hyperflow-job-executor"): undefined;
const { spawn } = require('child_process');
const redis = require('redis');
const fs = require('fs');
const log4js = require('log4js');
const pidtree = require('pidtree');
var pidusage = require('pidusage');
const si = require('systeminformation');
const path = require('path');
const { readEnv } = require('read-env');
const shortid = require('shortid');
const RemoteJobConnector = require('./connector');
const clog = require('./consoleLogger');

const {
    procfs,
    ProcfsError,
} = require('@stroncium/procfs');

const handlerId = shortid.generate();


/* 
** Function handleJob
** Parameters:
** - taskId: unique task identifier
** - rcl: redis client
*/
async function handleJob(taskId, rcl, message) {
    // Configure remote job worker
    let wfId = taskId.split(':')[1];
    let connector = new RemoteJobConnector(rcl, wfId);

    // time interval (ms) at which to probe and log metrics
    const probeInterval = process.env.HF_VAR_PROBE_INTERVAL || 2000; 

    // **Experimental**: add job info to Redis "hf_all_jobs" set
    var allJobsMember = taskId + "#" + process.env.HF_LOG_NODE_NAME + "#" + 
        process.env.HF_VAR_COLLOCATION_TYPE + "#" + process.env.HF_VAR_COLLOCATION_SIZE;
    rcl.sadd("hf_all_jobs", allJobsMember, function(err, ret) { if (err) clog.error(err); });

    // increment task acquisition counter
    async function acquireTask(rcl, taskId) {
        return new Promise(function (resolve, reject) {
            rcl.incr(taskId + '_acqCount', function(err, reply) {
                (err) ? reject(err) : resolve(reply);
            });
        });
    }

    // get job message from Redis
    var getJobMessage = async function (rcl, taskId, timeout) {
        return new Promise(function (resolve, reject) {
            const jobMsgKey = taskId + "_msg";
            rcl.brpoplpush(jobMsgKey, jobMsgKey, timeout, function (err, reply) {
                err ? reject(err): resolve(reply);
            });
        });
    }

    // send notification about job completion to Redis
    // 'code' is the job's exit code
    var notifyJobCompletion = async function (rcl, taskId, code) {
        return connector.notifyJobCompletion(taskId, code);
    }

    // check if job has already completed
    var hasCompleted = async function(rcl, taskId) {
        return new Promise((resolve, reject) => {
            let wfId = taskId.split(':')[1];
            var key = "wf:" + wfId + ":completedTasks";
            rcl.sismember(key, taskId, function(err, hasCompleted) {
                err ? reject(err): resolve(hasCompleted);
            });
        });
    }


    var jm;  // parsed job message

    // Process metrics monitoring. Pollers reschedule themselves via setTimeout;
    // all pending timers are cancelled when the job process closes, so a poller
    // hitting an already-exited pid is a rare race, not an error.
    var monitoring = false;
    var monitorTimers = new Set();

    function scheduleProbe(fn, pid) {
        if (!monitoring) return;
        let timer = setTimeout(() => { monitorTimers.delete(timer); fn(pid); }, probeInterval);
        monitorTimers.add(timer);
    }

    function stopMonitoring() {
        monitoring = false;
        monitorTimers.forEach(clearTimeout);
        monitorTimers.clear();
        pidusage.clear();
    }

    // logging basic process info from the procfs
    function logProcInfo(pid) {
        // log process command line
        try {
            let cmdInfo = { "pid": pid, "name": jm["name"], "command": procfs.processCmdline(pid) };
            logger.info("command:", JSON.stringify(cmdInfo));
        } catch (error) {
            if (error.code === ProcfsError.ERR_NOT_FOUND) {
                logger.debug(`process ${pid} does not exist`);
            }
        }

        // periodically log process IO
        function logProcIO(pid) {
            if (!monitoring) return;
            try {
                let ioInfo = procfs.processIo(pid);
                ioInfo.pid = pid;
                ioInfo.name = jm["name"];
                logger.info("IO:", JSON.stringify(ioInfo));
                scheduleProbe(logProcIO, pid);
            } catch (error) {
                if (error.code === ProcfsError.ERR_NOT_FOUND) {
                    logger.debug(`process ${pid} does not exist (this is okay)`);
                }
            }
        }
        logProcIO(pid);

        function logProcNetDev(pid) {
            if (!monitoring) return;
            try {
                let netDevInfo = procfs.processNetDev(pid);
                logger.info("NetDev: pid:", pid, JSON.stringify(netDevInfo));
                scheduleProbe(logProcNetDev, pid);
            } catch (error) {
                if (error.code === ProcfsError.ERR_NOT_FOUND) {
                    logger.debug(`process ${pid} does not exist (this is okay)`);
                }
            }
        }
        logProcNetDev(pid);

        function logPidUsage(pid) {
            if (!monitoring) return;
            pidusage(pid, function (err, stats) {
                if (err) {
                    logger.debug(`pidusage error ${err.code} for process ${pid}`);
                    return;
                }
                // stats: { cpu (%), memory (bytes), ppid, pid,
                //          ctime (ms user+system), elapsed (ms), timestamp (ms) }
                logger.info("Procusage: pid:", pid, JSON.stringify(stats));
                scheduleProbe(logPidUsage, pid);
            });
        }
        logPidUsage(pid);
    }

    var numRetries = process.env.HF_VAR_NUMBER_OF_RETRIES || 1;
    var backoffSeed = process.env.HF_VAR_BACKOFF_SEED || 10;

    // Rewrite command args by adding path to input directory if one is defined
    function addDirToCommand(jobIns, args, dir) {
        let newArgs = args;
        let changed = false;
        jobIns.forEach(function(jobInput) {
            if (jobInput.workflow_input) { // this is a workflow input
                newArgs.forEach(function(arg, idx) {
                    if (arg === jobInput.name) {
                        newArgs[idx] = path.join(dir, arg);
                        changed = true;
                    }
                });
            }
        });
        if (changed) {
            clog.debug("INPUT_DIR provided, command rewritten:", jm["executable"], newArgs);
            logger.info("INPUT_DIR provided, command rewritten:", jm["executable"], newArgs);
        }
        return newArgs;
    }

    async function executeJob(jm, attempt) {
        return new Promise((resolve, reject) => {
            if (process.env.HF_VAR_DRY_RUN === "1") {
                clog.info("DRY RUN...")
                return resolve(0);
            }
            var stdoutStream, stderrStream;

            let options = {};
            if (jm.shell) { 
                options = {shell: true}; 
            }

            let commandArgs = jm["args"];
            let jobIns = jm["inputs"];
            if (inputDir) {
                commandArgs = addDirToCommand(jobIns, commandArgs, inputDir);
            }

            // monotonic: the wall clock can step backwards and make short jobs
            // report a negative duration
            const jobStartTime = performance.now();
            const cmd = spawn(jm["executable"], commandArgs, options);
            let targetPid = cmd.pid;
            cmd.stdout.pipe(stdoutLog);
            cmd.stderr.pipe(stderrLog);

            monitoring = true;
            logProcInfo(targetPid);
            logger.info('job started:', jm["name"]);
            const c = clog.color;
            clog.info(c.dim('[hf-job]'), c.started('started:'), c.task(jm["name"]),
                      c.dim('(' + taskId + ')'));

            var sysinfo = {};

            // log system information
            si.cpu().
                then(data => {
                    sysinfo.cpu = data;
                }).
                then(si.mem).
                then(data => {
                    sysinfo.mem = data;
                }).
                then(data => logger.info("Sysinfo:", JSON.stringify(sysinfo))).
                catch(err => logger.error("sysinfo error:", err));

            // make sure info about all child processes is logged (checks periodically for new pids)
            var allpids = {}
            function addPidTree(pid) {
                if (!monitoring) return;
                pidtree(targetPid, function (err, pids) {
                    if (!pids || !monitoring) return;
                    pids.map(p => {
                        if (!allpids[p]) {
                            allpids[p] = "ok";
                            logProcInfo(p);
                        }
                    });
                    let timer = setTimeout(() => { monitorTimers.delete(timer); addPidTree(pid); }, 1000);
                    monitorTimers.add(timer);
                });
            }
            addPidTree(targetPid);

            // redirect process' stdout to a file
            let stdoutRedir = jm["stdout"] || jm["stdoutAppend"];
            if (stdoutRedir) {
                let f = jm["stdout"] ? 'w' : 'a'; // truncate or append file
                stdoutStream = fs.createWriteStream(stdoutRedir, {flags: f});
                cmd.stdout.pipe(stdoutStream);
            }

            // redirect process' stderr to a file
            let stderrRedir = jm["stderr"] || jm["stderrAppend"];
            if (stderrRedir) {
                let f = jm["stderr"] ? 'w' : 'a'; // truncate or append file
                stderrStream = fs.createWriteStream(stderrRedir, {flags: f});
                cmd.stderr.pipe(stderrStream);
            }

            // job stdout/stderr is captured to log files (see pipes above);
            // echo it to the console only at debug level
            if (clog.isDebug) {
                cmd.stdout.on('data', (data) => {
                    clog.debug(`stdout: ${data}`);
                });

                cmd.stderr.on('data', (data) => {
                    clog.debug(`stderr: ${data}`);
                });
            }

            cmd.on('close', async(code) => {
                stopMonitoring();
                clog.info(c.dim('[hf-job]'),
                          code != 0 ? c.failed('failed:') : c.finished('finished:'),
                          c.task(jm["name"]), c.dim('(' + taskId + ')'),
                          code != 0 ? c.failed('exit=' + code) : c.dim('exit=0'),
                          c.time('time=' + ((performance.now() - jobStartTime) / 1000).toFixed(1) + 's'));
                if (code != 0) {
                    logger.info("job failed (try " + attempt + "): '" + jm["executable"], jm["args"].join(' ') + "'");
                } else {
                    logger.info('job successful (try ' + attempt + '):', jm["name"]);
                }
                logger.info('job exit code:', code);

                // retry the job
                if (code !=0 && numRetries-attempt > 0) {
                    logger.info('Retrying job, number of retries left:', numRetries-attempt);
                    cmd.removeAllListeners();
                    // need to recreate write streams to log files for the retried job
                    stdoutLog = fs.createWriteStream(stdoutfilename, {flags: 'a'});
                    stderrLog = fs.createWriteStream(stderrfilename, {flags: 'a'});
                    var factor = attempt > 5 ? 5: attempt;
                    var backoffDelay = Math.floor(Math.random() * backoffSeed * factor); 
                    logger.info('Backoff delay:', backoffDelay, 's');
                    setTimeout(function() { resolve(executeJob(jm, attempt+1)); }, backoffDelay*1000);
                } else {
                    resolve(code);
                }
            });
        });
    }

    async function waitForInputs(jobInputFiles, maxRetries) {
        return new Promise((resolve, reject) => {
            let files = jobInputFiles.map(file => file.path).slice();
            var filesToWatch = jobInputFiles.slice();
            var filesReady = [];
            var numRetries = 0;
        
            var checkFiles = function() {
                var filesChecked = 0;
                var nFilesLeft = filesToWatch.length;

                if (numRetries > maxRetries) {
                    logger.info("Error waiting for input files", files);
                    return reject("Error waiting for input files", files);
                }

                //logger.info("Waiting for input files: (" + numRetries + ")", files);
                logger.info('waitingForFiles (' + numRetries + '): { "timestamp":', Date.now() +
                            ', "waitingForFiles":', JSON.stringify(filesToWatch.map(f => f.path)) + ', "filesReady":', 
                            JSON.stringify(filesReady), "}");
                            
                numRetries++;
                
                var filesFoundIdx = [];
                filesToWatch.forEach((file, i) => {
                    let fileStats;
                    try {
                        fileStats = fs.statSync(file.path);
                    } catch (err) { }

                    if (fileStats && (!file.size || file.size == fileStats.size)) {
                        filesChecked++;
                        filesReady.push({"file": file.path, "readTime": Date.now()});
                        filesFoundIdx.push(i);
                        delete filesToWatch[i];
                    }
                });
                filesToWatch = filesToWatch.filter(f => { return f; });

                if (filesFoundIdx.length) {
                    //filesToWatch.forEach((_, i) => filesToWatch.splice(i, 1));
                    logger.info('filesReady (' + numRetries + '): { "timestamp":', Date.now() +
                                ', "waitingForFiles":', JSON.stringify(filesToWatch) + ', "filesReady":', 
                                JSON.stringify(filesReady), "}");
                }

                if (filesToWatch.length == 0) {
                    logger.info("All input files ready!");
                    return resolve();
                } else {
                    const t = Math.pow(2, numRetries)+1000;
                    setTimeout(() => {
                        checkFiles();
                    }, t);
                }
            }

            checkFiles();
        });
    }

    // check if working directory is set
    if (process.env.HF_VAR_WORK_DIR) {
        process.chdir(process.env.HF_VAR_WORK_DIR);
    } else if (fs.existsSync("/work_dir")) {
        process.chdir("/work_dir");
    }

    var workDir = process.cwd();
    var logDir = process.env.HF_VAR_LOG_DIR || (workDir + "/logs-hf");
    var inputDir = process.env.HF_VAR_INPUT_DIR;
    var outputDir = process.env.HF_VAR_OUTPUT_DIR; 
    
    // make sure log directory is created
    try { fs.mkdirSync(logDir); } catch (err) {}
    fs.statSync(logDir);

    const loglevel = process.env.HF_VAR_LOG_LEVEL || 'info';
    const logfilename = logDir + '/task-' + taskId.replace(/:/g, '__') + '@' + handlerId + '.log';
    const stdoutfilename = logDir + '/task-' + taskId.replace(/:/g, '__') + '@' + handlerId + '__stdout.log';
    const stderrfilename = logDir + '/task-' + taskId.replace(/:/g, '__') + '@' + handlerId + '__stderr.log';
    var stdoutLog = fs.createWriteStream(stdoutfilename, {flags: 'w'});
    var stderrLog = fs.createWriteStream(stderrfilename, {flags: 'w'});
    const enableNethogs = process.env.HF_VAR_ENABLE_NETHOGS=="1";
    const nethogsfilename = logDir + '/task-' + taskId.replace(/:/g, '__') + '@' + handlerId + '__nethogs.log';

    log4js.configure({
        appenders: { hftrace: { type: 'file', filename: logfilename} },
        categories: { default: { appenders: ['hftrace'], level: loglevel } }
    });

    const logger = log4js.getLogger('hftrace');

    // log all environment variables starting with HF_LOG_
    const envLog = readEnv("HF_LOG");
    logger.info("Environment variables (HF_LOG):", JSON.stringify(envLog));

    //var rcl = redis.createClient(redisUrl);

    logger.info('handler started, (ID: ' + handlerId + ')');

    // 0. Detect multiple task acquisitions
    let totalAcq = await acquireTask(rcl, taskId);
    if (totalAcq > 1) {
        let beforeAcq = totalAcq - 1;
        logger.warn('Task was already acquired', beforeAcq.toString(), 'times');
    }

    // 1. Check if this job has already been completed -- useful in Kubernetes
    // where sometimes a succesful job can be restarted for unknown reason
    var jobHasCompleted = await hasCompleted(rcl, taskId);

    if (jobHasCompleted) {
        logger.warn("Warning: unexpected restart of job", taskId, 
                    "(already succesfully completed)!");
        log4js.shutdown(function () { return 0; });
        return;
    }

    // 2. Get job message from Redis if not given as an handeJob argument
    if (message === null) {
        let jobMessage = null;
        try {
            jobMessage = await getJobMessage(rcl, taskId, 0);
        } catch (err) {
            clog.error(err);
            logger.error(err);
            throw err;
        }
        jm = JSON.parse(jobMessage);
    } else {
        jm = message
    }

    // negotiate completion-notification transport from the parsed job message
    connector.transport = (jm.completionTransport === "stream") ? "stream" : "set";

    logger.info('jobMessage: ', JSON.stringify(jm))
    clog.debug("Received job message:", JSON.stringify(jm));

    // create arrays of input and output file names; if inpuDir/outputDir is present, 
    // add path to it (for files which are flagged as 'workflow_input'/'workflow_output')
    var inputFiles = jm.inputs;
    var outputFiles = jm.outputs;
    inputFiles.forEach((input) => {
        input.path = inputDir && input.workflow_input ? path.join(inputDir, input.name) : input.name;
    });
    outputFiles.forEach((output) => {
        output.path = outputDir && output.workflow_output ? path.join(outputDir, output.name) : output.name;
    });

    // 3. Check/wait for input files (useful in some distributed file systems)
    if (process.env.HF_VAR_WAIT_FOR_INPUT_FILES=="1" && jm.inputs && jm.inputs.length) {
        try {
            await waitForInputs(inputFiles, process.env.HF_VAR_FILE_WATCH_NUM_RETRIES || 10);
        } catch(err) {
            throw err;
        }
    }

    // 4. turn on network IO monitoring using nethogs
    let nethogs;
    if (enableNethogs) {
        var nethogsStream = fs.createWriteStream(nethogsfilename, {flags: 'w'});
        nethogs = spawn("nethogs-wrapper.py");
        nethogs.stdout.pipe(nethogsStream);
        nethogs.on('error', function(err){ 
            logger.error("nethogs execution error:", err);
        });
    }

    // 5. Execute job
    logger.info("Job command: '" + jm["executable"], jm["args"].join(' ') + "'");
    let jobExitCode = await executeJob(jm, 1);

    // Notify job completion to HyperFlow
    try {
        await notifyJobCompletion(rcl, taskId, jobExitCode);
        //console.log(Date.now(), 'job ended');
    } catch (err) {
        clog.error("Redis notification failed", err);
        logger.error("Redis notification failed: " + err);
        throw err;
    }

    // log info about input/output files
    var getFileSizeObj = function (fileName, filePath) {
        var size = -1;
        try {
            var stats = fs.statSync(filePath);
            size = stats["size"];
        } catch (err) { }
        return {[fileName]: size};
    }

    var inputsLog = inputFiles.map(inFile => getFileSizeObj(inFile.name, inFile.path));
    var outputsLog = outputFiles.map(outFile => getFileSizeObj(outFile.name, outFile.path));

    logger.info("Job inputs:", JSON.stringify(inputsLog));
    logger.info("Job outputs:", JSON.stringify(outputsLog));

    // **Experimental**: remove job info from Redis "hf_all_jobs" set
    rcl.srem("hf_all_jobs", allJobsMember, function (err, ret) { if (err) clog.error(err); });

    logger.info('handler finished, code=', jobExitCode);

    // 6. Perform cleanup operations
    if (nethogs !== undefined) {
        nethogs.stdin.pause();
        nethogs.kill();
    }
    log4js.shutdown(function (err) {
        if (err !== undefined) {
            logger.error("log4js shutdown error:", err);
        }
    });
    pidusage.clear();

    return jobExitCode;
}

exports.handleJob = handleJob;
