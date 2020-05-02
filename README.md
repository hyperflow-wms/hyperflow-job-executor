# HyperFlow Job executor

![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/hyperflow-wms/hyperflow-job-executor?include_prereleases&sort=date)

This is a basic HyperFlow job executor that uses local directory path to read and write files, and Redis for communication with the HyperFlow engine.

## Adding the executor to a Docker image
- Add installation of Node.js 10.x or higher 
- Install the executor package: `npm install https://github.com/hyperflow-wms/hyperflow-job-executor/archive/master.tar.gz`
- Add `node_modules/.bin` to `PATH` (adds `hflow-job-execute` command)

## Running jobs
Jobs submitted from HyperFlow function `function(ins, outs, context, cb)`  must be run using the following command: `hflow-job-execute <taskId> <redis_url>`, where both parameters are available in HyperFlow functions as `context.taskId` and `context.redis_url`, respectively. The actual job command to be run by the executor should be sent via Redis, see `hyperflow/examples/RemoteJobs` for more details.

## Logging
The executor creates log files in directory `<work_dir>/logs-hf` that contain:
- command used to execute the job
- `stdout` and `stderr` of the job
- metrics (CPU/memory/IO/network usage)
- events (job start/end)
- system information (e.g. hardware configuration)

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
