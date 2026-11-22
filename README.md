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

A release is one command, run on `master` once the changes it should contain
are merged:

```bash
gh release create v<version> --target master --generate-notes
```

Every release, prereleases included, is cut from `master`.

The release tag is the only place the version is set. Between releases
`package.json` holds the placeholder version `0.0.0-dev`, and nobody commits a
version bump. Publishing the release starts the CI workflow
(`.github/workflows/ci.yml`), whose publish job:

1. checks that the tagged commit is on `master`, and fails otherwise;
2. checks that the tag is `v` followed by a semantic version
   ([semver.org](https://semver.org)), for example `v1.4.3` or `v1.5.0-rc.1`,
   and fails otherwise;
3. writes the tag's version into `package.json` and `package-lock.json` with
   `npm version --no-git-tag-version`, in CI's checkout only;
4. refuses to republish a version that is already on npm;
5. runs `npm publish`. A release version gets the `latest` dist-tag, so
   `npm install @hyperflow/job-executor` installs it. A prerelease such as
   `1.5.0-rc.1` gets the `next` dist-tag and is installed only when asked
   for, by its version or as `@next`. npm refuses to move `latest` to a
   version lower than the newest one published, so a release below it fails.

If the publish job fails on a release, re-run the failed run. GitHub allows a
re-run for 30 days after the run, and the re-run uses the same commit and tag:

```bash
gh run list --workflow ci.yml --event release   # find the run's id
gh run rerun <run-id>
```

Alternatively, delete the release and create it again on the same tag.
`gh release delete` keeps the tag unless it is given `--cleanup-tag`, and
`--verify-tag` makes `gh release create` use the existing tag instead of
creating one:

```bash
gh release delete v<version> --yes
gh release create v<version> --verify-tag --generate-notes
```

Both run the workflow file as it is at the tagged commit, so a failure caused
by the workflow itself needs a fix on `master` and a release with a new
version. A release whose commit is not on `master` cannot be fixed this way:
delete the release together with its tag
(`gh release delete v<version> --cleanup-tag`) and create it again with
`--target master`.

There is no npm token. CI authenticates with npm through trusted publishing,
exchanging a GitHub OIDC identity for publish rights, which also attaches a
provenance attestation to the package. npmjs.com holds the matching entry:
package settings, trusted publisher, `hyperflow-wms/hyperflow-job-executor`,
workflow `ci.yml`. Renaming this workflow file breaks publishing until that
entry is updated to match.

Worker images that install this package (1000genome, Montage 6, SoyKB) pick up
the new version through Renovate pull requests in their own repositories;
merging one of those builds and pushes the corresponding image.

Only a release publishes. To rehearse the publish without releasing anything,
run the CI workflow manually, for example with
`gh workflow run ci.yml --ref master`. A manual run always runs
`npm publish --dry-run` on the version in `package.json` (`0.0.0-dev` on
`master`), also when it is dispatched on a tag, and prints the exact file list
that would be uploaded.

`tests/check-release-tag.test.js` runs the workflow's version and publish steps
locally against sample tags, with a fake `npm` for the publish step, and the
check that the tagged commit is on `master` against a local git repository.
