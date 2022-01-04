#!/usr/bin/env node
// Executor of 'jobs' using the Redis task status notification mechanism
const { Connection } = require('@hflow-monitoring/hyperflow-metrics-listeners').Client;
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
async function handleJob(taskId, rcl) { 
    // Configure remote job worker
    let wfId = taskId.split(':')[1];
    let connector = new RemoteJobConnector(rcl, wfId);

    // time interval (ms) at which to probe and log metrics
    const probeInterval = process.env.HF_VAR_PROBE_INTERVAL || 2000; 

    // **Experimental**: add job info to Redis "hf_all_jobs" set
    var allJobsMember = taskId + "#" + process.env.HF_LOG_NODE_NAME + "#" + 
        process.env.HF_VAR_COLLOCATION_TYPE + "#" + process.env.HF_VAR_COLLOCATION_SIZE;
    rcl.sadd("hf_all_jobs", allJobsMember, function(err, ret) { if (err) console.log(err); });

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


    var pids = {} // pids of the entire pid tree (in case the main process starts child processes)
    var jm;  // parsed job message
    const metricsListenerConnection = new Connection(process.env.METRICS_LISTENER_URL);
    var metricBase; // metric object with all common fields set
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
                let io = {
                    ...metricBase,
                    time: new Date(), 
                    pid: pid + '', 
                    parameter: 'io', 
                    value: ioInfo
                }
                metricsListenerConnection.send('metric', io);
                logger.info("IO:", JSON.stringify(ioInfo));
                setTimeout(() => logProcIO(pid), probeInterval);
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
                let network = {
                    ...metricBase,
                    time: new Date(stats.timestamp), 
                    pid: pid + '', 
                    parameter: 'network', 
                    value: JSON.stringify(netDevInfo)
                }
                //netDevInfo.pid = pid;
                //netDevInfo.name = jm["name"];
                metricsListenerConnection.send('metric', network);
                logger.info("NetDev: pid:", pid, JSON.stringify(netDevInfo));
                setTimeout(() => logProcNetDev(pid), probeInterval);
            } catch (error) {
                if (error.code === ProcfsError.ERR_NOT_FOUND) {
                    //console.error(`process ${pid} does not exist (this is okay)`);
                }
            }
        }
        logProcNetDev(pid);

        logPidUsage = function(pid) {
            pidusage(pid, function (err, stats) {
                if (err) {
                    console.error(`pidusage error ${err.code} for process ${pid}`);
                    return;
                }
                //console.log(stats);
                // => {
                //   cpu: 10.0,            // percentage (from 0 to 100*vcore)
                //   memory: 357306368,    // bytes
                //   ppid: 312,            // PPID
                //   pid: 727,             // PID
                //   ctime: 867000,        // ms user + system time
                //   elapsed: 6650000,     // ms since the start of the process
                //   timestamp: 864000000  // ms since epoch
                // }
                let cpu = { 
                    ...metricBase, 
                    time: new Date(stats.timestamp), 
                    pid: pid + '', 
                    parameter: 'cpu', 
                    value: stats.cpu
                };
                let memory = {
                    ...metricBase, 
                    time: new Date(stats.timestamp), 
                    pid: pid + '', 
                    parameter: 'memory', 
                    value: stats.memory
                }
                let ctime = {
                    ...metricBase, 
                    time: new Date(stats.timestamp), 
                    pid: pid + '', 
                    parameter: 'ctime', 
                    value: stats.ctime
                }
                metricsListenerConnection.send('metric', cpu);
                metricsListenerConnection.send('metric', memory);
                metricsListenerConnection.send('metric', ctime);
                logger.info("Procusage: pid:", pid, JSON.stringify(stats));
                setTimeout(() => logPidUsage(pid), probeInterval);
            });
        }
        logPidUsage(pid);
    }

    var numRetries = process.env.HF_VAR_NUMBER_OF_RETRIES || 1;
    var backoffSeed = process.env.HF_VAR_BACKOFF_SEED || 10;

    async function executeJob(jm, attempt) {
        return new Promise((resolve, reject) => {
            if (process.env.HF_VAR_DRY_RUN) {
                console.log("DRY RUN...")
                return resolve(0);
            }
            var stdoutStream, stderrStream;

            let options = {};
            if (jm.shell) { 
                options = {shell: true}; 
            }

            const cmd = spawn(jm["executable"], jm["args"], options);
            let targetPid = cmd.pid;
            cmd.stdout.pipe(stdoutLog);
            cmd.stderr.pipe(stderrLog);

            logProcInfo(targetPid);
            // send jobStart event
            const job_start_event = {
                ...metricBase,
                time: new Date(), 
                parameter: 'event', 
                value: 'jobStart'
            }
            metricsListenerConnection.send('metric', job_start_event);
            logger.info('job started:', jm["name"]);

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
                then(data =>{
                    let sysInfo = {
                        jobId: taskId.split(':').slice(0, 3).join('-'),
                        cpu: sysinfo.cpu,
                        mem: sysinfo.mem
                    }
                    metricsListenerConnection.send('sys_info', sysInfo);
                    logger.info("Sysinfo:", JSON.stringify(sysinfo))
                }).
                catch(err => console.err(error));

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

                // send jobEnd event
                const job_end_event = {
                    ...metricBase,
                    time: new Date(), 
                    parameter: 'event', 
                    value: 'jobEnd'
                }
                metricsListenerConnection.send('metric', job_end_event);
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
                    setTimeout(function() { executeJob(jm, attempt+1); }, backoffDelay*1000);
                } else {
                    resolve(code);
                }
            });
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

    // check if working directory is set
    if (process.env.HF_VAR_WORK_DIR) {
        process.chdir(process.env.HF_VAR_WORK_DIR);
    } else if (fs.existsSync("/work_dir")) {
        process.chdir("/work_dir");
    }

    var workDir = process.cwd();
    var logDir = process.env.HF_VAR_LOG_DIR || (workDir + "/logs-hf");
    var inputDir = process.env.HF_VAR_INPUT_DIR || workDir;
    var outputDir = process.env.HF_VAR_OUTPUT_DIR || workDir;
    
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

    const handlerStartTime = new Date();
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

    // 2. Get job message
    let jobMessage = null;
    try {
        jobMessage = await getJobMessage(rcl, taskId, 0);
    } catch (err) {
        console.error(err);
        logger.error(err);
        throw err;
    }
    logger.info('jobMessage: ', jobMessage)
    console.log("Received job message:", jobMessage);
    jm = JSON.parse(jobMessage);

    const jobDescription = {
        workflowName: process.env.HF_WORKFLOW_NAME || 'unknown',
        size: 1, // TODO
        version: '1.0.0',
        hyperflowId: taskId.split(':')[0],
        jobId: taskId.split(':').slice(0, 3).join('-'),
        env: {
            podIp: process.env.HF_LOG_POD_IP || "unknown",
            nodeName: process.env.HF_LOG_NODE_NAME || "unknown",
            podName: process.env.HF_LOG_POD_NAME || "unknown",
            podServiceAccount: process.env.HF_LOG_POD_SERVICE_ACCOUNT || "default",
            podNameSpace: process.env.HF_LOG_POD_NAMESPACE || "default"
        },
        executable: jm['executable'],
        args: jm['args'],
        nodeName: process.env.HF_LOG_NODE_NAME || 'unknown',
        inputs: jm['inputs'],
        outputs: jm['outputs'],
        name: jm['name'],
        command: jm['executable'] + ' ' + jm["args"].join(' ')
    }
    metricBase = {
        workflowId: taskId.split(':').slice(0, 2).join('-'),
        jobId: taskId.split(':').slice(0, 3).join('-'),
        name: jm['name'],
    }
    // send handler_start event (has to be done after connection has been established)
    const handler_start_event = {
        ...metricBase,
        time: handlerStartTime, 
        parameter: 'event', 
        value: 'handlerStart'
    }
    metricsListenerConnection.send('metric', handler_start_event);
    metricsListenerConnection.send('job_description', jobDescription);

    // 3. Check/wait for input files
    if (process.env.HF_VAR_WAIT_FOR_INPUT_FILES=="1" && jm.inputs && jm.inputs.length) {
        var files = jm.inputs.map(input => input.name).slice();
        try {
            await waitForInputs(files, process.env.HF_VAR_FILE_WATCH_NUM_RETRIES || 10);
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
        console.error("Redis notification failed", err);
        logger.error("Redis notification failed: " + err);
        throw err;
    }

    // log info about input/output files
    var getFileSizeObj = function (file) {
        var size = -1;
        try {
            var stats = fs.statSync(file);
            size = stats["size"];
        } catch (err) { }
        var obj = {};
        obj[file] = size;
        return obj;
    }

    var inputFiles = jm.inputs.map(input => input.name).slice();
    var outputFiles = jm.outputs.map(output => output.name).slice();
    var inputsLog = inputFiles.map(inFile => getFileSizeObj(inFile));
    var outputsLog = outputFiles.map(outFile => getFileSizeObj(outFile));

    logger.info("Job inputs:", JSON.stringify(inputsLog));
    logger.info("Job outputs:", JSON.stringify(outputsLog));

    // **Experimental**: remove job info from Redis "hf_all_jobs" set
    rcl.srem("hf_all_jobs", allJobsMember, function (err, ret) { if (err) console.log(err); });

    // send handler_finished event
    const handler_finished_event = {
        ...metricBase,
        time: new Date(), 
        parameter: "event", 
        value: "handlerEnd"
    };
    metricsListenerConnection.send('metric', handler_finished_event);

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
    metricsListenerConnection.close()
    return jobExitCode;
}

exports.handleJob = handleJob;
