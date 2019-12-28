# HyperFlow Job executor

![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/hyperflow-wms/hyperflow-job-executor?include_prereleases&sort=date)

This is a basic HyperFlow job executor that uses local directory path to read and write files, and Redis for communication with the HyperFlow engine.

## Adding the executor to a Docker image
- Add installation of Node.js 10.x or higher 
- Install the executor package: `npm install https://github.com/hyperflow-wms/hyperflow-job-executor/archive/master.tar.gz`
- Add `node_modules/.bin` to `PATH` (adds `hflow-job-execute` command)

## Running jobs
Jobs submitted from HyperFlow function `function(ins, outs, context, cb)`  must be run using the following command: `hflow-job-execute <taskId> <redis_url>`, where both parameters are available in HyperFlow functions as `context.taskId` and `context.redis_url`, respectively. The actual job command to be run by the executor should be sent via Redis, see `hyperflow/examples/RemoteJobs` for more details.

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
