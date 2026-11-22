/**
 * Tests for how the job executor reports a task's completion to the engine.
 *
 * connector.js, RemoteJobConnector's completion transport ("set" or "stream"):
 *  1. transport === "stream" -> exactly one XADD with the exact wire-format
 *     args (hf:<hfId>:completions MAXLEN ~ 100000 * taskId <taskId> code
 *     <code>), and no SADD calls at all.
 *  2. default transport ("set", i.e. unset) -> the legacy two-SADD pair
 *     (result set + completed-notification queue), and no XADD call.
 *  3. explicit transport "set" -> the same two-SADD pair.
 *
 * handler.js, handleJob with a job whose program cannot be started:
 *  4. the executable does not exist -> handleJob returns 127, the job's
 *     attempt ends once, with exit code 127, the completion is reported
 *     exactly once with exit code 127, and no process monitoring runs.
 *  5. the executable is not executable -> the same.
 *
 * Self-contained: uses a fake redis client and stand-ins for the npm
 * packages handler.js requires, so it needs no live services and no
 * npm install.
 * Run with:  node tests/connector.test.js
 */
const assert = require('assert');
const fs = require('fs');
const Module = require('module');
const os = require('os');
const path = require('path');
const RemoteJobConnector = require('../connector.js');

class FakeRedis {
    constructor() {
        this.xaddCalls = [];
        this.saddCalls = [];
    }

    // xadd(key, 'MAXLEN', '~', '100000', '*', field1, val1, field2, val2, ..., cb)
    xadd(...args) {
        const cb = args.pop();
        this.xaddCalls.push(args);
        setImmediate(cb, null, "0-1");
    }

    sadd(key, member, cb) {
        this.saddCalls.push([key, member]);
        setImmediate(cb, null, 1);
    }

    // the other calls handleJob makes
    incr(key, cb) {
        setImmediate(cb, null, 1);
    }

    sismember(key, member, cb) {
        setImmediate(cb, null, 0);
    }

    srem(key, member, cb) {
        setImmediate(cb, null, 1);
    }
}

// Stand-ins for the npm packages handler.js requires, which are not
// installed. The log4js logger records its lines in logLines, and the
// process-monitoring calls record the pid they are given in monitoredPids.
const logLines = [];
const monitoredPids = [];
const log = (...args) => { logLines.push(args.join(" ")); };
const logger = { trace: log, debug: log, info: log, warn: log, error: log, fatal: log };
const stubs = {
    'redis': {},
    'log4js': {
        configure: () => {},
        getLogger: () => logger,
        shutdown: (cb) => { if (cb) cb(); },
    },
    'pidtree': (pid, cb) => { monitoredPids.push(pid); setImmediate(cb, null, []); },
    'pidusage': Object.assign((pid, cb) => { monitoredPids.push(pid); setImmediate(cb, null, {}); },
                              { clear: () => {} }),
    'systeminformation': { cpu: async () => ({}), mem: async () => ({}) },
    'read-env': { readEnv: () => ({}) },
    'shortid': { generate: () => "test" },
    '@stroncium/procfs': {
        procfs: {
            processCmdline: (pid) => { monitoredPids.push(pid); return ""; },
            processIo: (pid) => { monitoredPids.push(pid); return {}; },
            processNetDev: (pid) => { monitoredPids.push(pid); return {}; },
        },
        ProcfsError: { ERR_NOT_FOUND: "ERR_NOT_FOUND" },
    },
};

// require handler.js with the stand-ins in place of the npm packages
function requireHandler() {
    const load = Module._load;
    Module._load = function (request) {
        return Object.prototype.hasOwnProperty.call(stubs, request) ?
            stubs[request] : load.apply(this, arguments);
    };
    try {
        return require('../handler.js');
    } finally {
        Module._load = load;
    }
}

async function testStreamTransport() {
    const rcl = new FakeRedis();
    const wfId = "1";
    const connector = new RemoteJobConnector(rcl, wfId);
    connector.transport = "stream";

    const taskId = "hf123:1:5:1";
    const code = 0;
    await connector.notifyJobCompletion(taskId, code);

    assert.strictEqual(rcl.xaddCalls.length, 1,
        "expected exactly one xadd call, got " + rcl.xaddCalls.length);
    assert.deepStrictEqual(rcl.xaddCalls[0],
        ["hf:hf123:completions", "MAXLEN", "~", "100000", "*", "taskId", taskId, "code", code],
        "xadd args do not match the exact wire format: " + JSON.stringify(rcl.xaddCalls[0]));
    assert.strictEqual(rcl.saddCalls.length, 0,
        "expected no sadd calls for stream transport, got " + JSON.stringify(rcl.saddCalls));
    console.log("ok 1 - transport=stream emits exactly one XADD with exact args, no SADD");
}

async function testDefaultSetTransport() {
    const rcl = new FakeRedis();
    const wfId = "1";
    const connector = new RemoteJobConnector(rcl, wfId);
    // transport left at its default (no assignment) -> must be "set"

    const taskId = "hf123:1:6:1";
    const code = "0";
    await connector.notifyJobCompletion(taskId, code);

    assert.strictEqual(connector.transport, "set", "default transport should be \"set\"");
    assert.strictEqual(rcl.xaddCalls.length, 0,
        "expected no xadd calls for default/set transport, got " + JSON.stringify(rcl.xaddCalls));
    assert.strictEqual(rcl.saddCalls.length, 2,
        "expected exactly two sadd calls (legacy pair), got " + rcl.saddCalls.length);
    assert.deepStrictEqual(rcl.saddCalls[0], [taskId, code],
        "first sadd should add the result into the taskId set: " + JSON.stringify(rcl.saddCalls[0]));
    assert.deepStrictEqual(rcl.saddCalls[1],
        ["wf:" + wfId + ":tasksPendingCompletionHandling", taskId],
        "second sadd should mark the task in the completed-notification queue: " + JSON.stringify(rcl.saddCalls[1]));
    console.log("ok 2 - default transport emits exactly the legacy two-SADD pair, no XADD");
}

async function testExplicitSetTransport() {
    const rcl = new FakeRedis();
    const wfId = "2";
    const connector = new RemoteJobConnector(rcl, wfId);
    connector.transport = "set";

    const taskId = "hf123:2:7:1";
    const code = "1";
    await connector.notifyJobCompletion(taskId, code);

    assert.strictEqual(rcl.xaddCalls.length, 0, "expected no xadd calls, got " + JSON.stringify(rcl.xaddCalls));
    assert.strictEqual(rcl.saddCalls.length, 2, "expected exactly two sadd calls");
    console.log("ok 3 - explicit transport=set behaves like the legacy default");
}

async function testStartFailure(handleJob, n, executable, what) {
    const rcl = new FakeRedis();
    const taskId = "hf123:1:" + n + ":1";
    logLines.length = 0;
    const jm = {
        name: "start-failure",
        executable: executable,
        args: ["x"],
        inputs: [],
        outputs: [],
        completionTransport: "stream",
    };
    const code = await handleJob(taskId, rcl, jm);
    // the child can emit 'close' after 'error'; give it time to arrive
    await new Promise((resolve) => setTimeout(resolve, 300));

    assert.strictEqual(code, 127, "handleJob should return 127, got " + code);
    assert.ok(logLines.some((line) => line.startsWith("job could not be started:")),
        "the start error was not logged: " + JSON.stringify(logLines));
    assert.deepStrictEqual(logLines.filter((line) => line.startsWith("job exit code:")),
        ["job exit code: 127"], "the attempt should end once, with exit code 127");
    assert.strictEqual(rcl.xaddCalls.length, 1,
        "expected exactly one completion, got " + JSON.stringify(rcl.xaddCalls));
    assert.deepStrictEqual(rcl.xaddCalls[0],
        ["hf:hf123:completions", "MAXLEN", "~", "100000", "*", "taskId", taskId, "code", 127],
        "completion does not carry exit code 127: " + JSON.stringify(rcl.xaddCalls[0]));
    assert.deepStrictEqual(monitoredPids, [],
        "process monitoring ran for a program that did not start: " + JSON.stringify(monitoredPids));
    console.log("ok " + n + " - " + what + ": completion reported once with exit code 127");
}

async function testHandlerStartFailures() {
    // run handleJob with its default settings, in a fresh work directory
    for (const name of Object.keys(process.env)) {
        if (name.startsWith("HF_VAR_")) delete process.env[name];
    }
    const startDir = process.cwd();
    const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "hf-job-executor-test-"));
    process.env.HF_VAR_WORK_DIR = workDir;
    const notExecutable = path.join(workDir, "not-executable");
    fs.writeFileSync(notExecutable, "#!/bin/sh\nexit 0\n", { mode: 0o644 });

    const { handleJob } = requireHandler();
    await testStartFailure(handleJob, 4, path.join(workDir, "no-such-program"),
                           "executable does not exist");
    await testStartFailure(handleJob, 5, notExecutable, "executable is not executable");

    process.chdir(startDir);
    fs.rmSync(workDir, { recursive: true, force: true });
}

async function main() {
    await testStreamTransport();
    await testDefaultSetTransport();
    await testExplicitSetTransport();
    await testHandlerStartFailures();
    console.log("all connector.test.js tests passed");
    process.exit(0);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
