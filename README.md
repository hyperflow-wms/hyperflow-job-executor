# HyperFlow Job executor

![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/hyperflow-wms/hyperflow-job-executor?include_prereleases&sort=date)

This is a basic HyperFlow job executor that uses local directory path to read and write files, and Redis (or RabbitMQ/Redis) for communication with the [HyperFlow engine](https://github.com/hyperflow-wms/hyperflow).

## Adding the executor to a Docker image
- Install Node.js 12.x or higher 
- Install the executor package: 
  * Latest version: `npm install -g @hyperflow/job-executor`
  * Specific version: `npm install -g @hyperflow/job-executor@X.Y.Z`
  * From master branch: `npm install -g https://github.com/hyperflow-wms/hyperflow-job-executor/archive/master.tar.gz`

## Running jobs (Redis only)
Jobs can be run with either of the following commands:
- `hflow-job-execute <taskId> <redisUrl>`, where `taskId` is a unique job identifier, while `redisUrl` is an URL to the Redis server where the actual job command is fetched from. Both parameters are available in HyperFlow functions as `context.taskId` and `context.redis_url`, respectively.
- `hflow-job-execute <redisUrl> -a -- <taskId>...` -- to run multiple jobs sequentially (useful for [agglomeration of small jobs](https://github.com/hyperflow-wms/hyperflow/wiki/Task-agglomeration)).

Jobs can be submitted e.g. using the HyperFlow function [`k8sCommand`](https://github.com/hyperflow-wms/hyperflow/blob/master/functions/kubernetes/k8sCommand.js). See [RemoteJobs example](https://github.com/hyperflow-wms/hyperflow/tree/master/examples/RemoteJobs) to learn more details.

## Running jobs with AMQP listener
The AMQP listener can be run with the following command:
- `hflow-job-listener.js`

Note that in order to run an executor following variables must be set with proper values:
- `RABBIT_HOSTNAME`
- `QUEUE_NAME`

More details about those configuration variables are in **Configuration** chapter.

## Tracing using OpenTelemetry
Use environmental variables to adjust the configuration of services.
- `HF_VAR_OPT_URL` (eg. `collector-gateway`): provide name under which collector should be accessible (process should be able to call http://{HF_VAR_OPT_URL}:4318/v1/traces')
- `HF_VAR_ENABLE_TRACING` (`0` or `1`): emit traces, spans, and metrics

## Logging
The executor creates log files in directory `<work_dir>/logs-hf` that contain:
- command used to execute the job
- `stdout` and `stderr` of the job
- metrics (CPU/memory/IO/network usage)
- events (job start/end)
- system information (e.g. hardware configuration)
- all environment variables starting with `HF_LOG_` -- a JSON object is logged following conventions from the [read-env](https://www.npmjs.com/package/read-env) package

## Configuration

The following environment variables can be used to adjust the behavior of the job executor:
- `HF_VAR_PROBE_INTERVAL` (default `2000`): time interval (in ms) at which to probe and log metrics.
- `HF_VAR_NUMBER_OF_RETRIES` (default `1`): how many times the job should be re-executed if it returns a non-zero exit code.  
- `HF_VAR_BACKOFF_SEED` (default `10`): factor used in calculating the backoff delay between retries.
- `HF_VAR_WAIT_FOR_INPUT_FILES`: if set to `1`, the executor will check if input files exist and wait for them (useful in systems where files are synchronized in an eventually consistent fashion).
- `HF_VAR_FILE_WATCH_NUM_RETRIES` (default `10`): how many times should the executor check for existence of the input files (with backoff waits in between).
- `HF_VAR_WORK_DIR`: path to the working directory where the job should be executed. If not set, `/work_dir` will be used if exists, otherwise the executor will not change the working directory.
- `HF_VAR_LOG_DIR`: path to the directory where log files should be written. If not set, `<work dir>/logs-hf` will be used.
- `HF_VAR_LOG_LEVEL` (default `info`): set logging level (`trace`, `debug`, `info`, `warn`, `error`, `fatal`).
- `HF_VAR_ENABLE_NETHOGS`: if set (to any value), logs from [nethogs](https://github.com/raboof/nethogs) will be written (experimental).
- `HF_VAR_DRY_RUN`: (for testing/debugging) if set to `1`, the executor will immediately return with job exit status `0` (success). 
- `HF_LOG_*`: all variables starting with `HF_LOG_` will be logged in the job log files
- `RABBIT_HOSTNAME`: RabbitMQ instance hostname. Can be supplied with basic auth credentials, for example: `username:password@rabbitmq.default`. 
When no username nor password are specified, default RabbitMQ guest:guest credentials are used. **Required in AMQP executor mode**
- `QUEUE_NAME` the name of the queue at which executor will wait for task messages. **Required in AMQP executor mode**

## Releasing

For quick and dirty developer releases

```bash
# Commit your changes
make dev-release
```

To release a proper version:

```bash
# Commit your changes
# Use npm version <arg>, to tag your changes and bump npm version
make release
```
