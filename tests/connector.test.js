/**
 * Regression tests for connector.js: RemoteJobConnector's completion-
 * notification transport negotiation (legacy "set" vs new "stream").
 *
 * Covers:
 *  1. transport === "stream" -> exactly one XADD with the exact wire-format
 *     args (hf:<hfId>:completions MAXLEN ~ 100000 * taskId <taskId> code
 *     <code>), and no SADD calls at all.
 *  2. default transport ("set", i.e. unset) -> the legacy two-SADD pair
 *     (result set + completed-notification queue), and no XADD call.
 *
 * Self-contained: uses a fake redis client, no live services.
 * Run with:  node tests/connector.test.js
 */
const assert = require('assert');
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

async function main() {
    await testStreamTransport();
    await testDefaultSetTransport();
    await testExplicitSetTransport();
    console.log("all connector.test.js tests passed");
    process.exit(0);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
