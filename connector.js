/**
 * Class for notifying HyperFlow's master about tasks' results.
 *
 * Details:
 *  1) exit code is added into set 'wf:[wfID]:completedTasks',
 *   where [wfID] is workflow ID,
 *  2) then task completion is marked by pushing 'code' into 'taskId' set.
 */
const clog = require('./consoleLogger');

class RemoteJobConnector {
    /**
     * Constructor.
     * @param {RedisClient} redisClient redis client
     * @param {string} wfId workflow ID
     */
    constructor(redisClient, wfId) {
        if (!(this instanceof RemoteJobConnector)) {
            return new RemoteJobConnector(redisClient, wfId);
        }
        this.rcl = redisClient;
        this.completedNotificationQueueKey = "wf:" + wfId + ":tasksPendingCompletionHandling";
        // completion-notification transport: "set" (legacy, default) or "stream".
        // Set by the caller (handler.js) from the parsed job message.
        this.transport = "set";
    }

    /**
     * Notify HyperFlow about remote job completion via the completions stream.
     * Emits exactly: XADD hf:<hfId>:completions MAXLEN ~ 100000 * taskId <taskId> code <code>
     * where hfId is the first ':'-separated segment of taskId.
     * @param {string} taskId task ID
     * @param {number} code exit code
     */
    async notifyJobCompletionStream(taskId, code) {
        let hfId = taskId.split(":")[0];
        let streamKey = "hf:" + hfId + ":completions";
        clog.debug("[RemoteJobConnector] Adding result", code, "of task", taskId, "to stream", streamKey);
        return new Promise((resolve, reject) => {
            this.rcl.xadd(streamKey, "MAXLEN", "~", "100000", "*", "taskId", taskId, "code", code,
                function (err, reply) {
                    err ? reject(err): resolve(reply);
                });
        });
    }

    /**
     * Notify HyperFlow about remote job completion.
     * @param {string} taskId task ID
     * @param {number} code exit code
     */
    async notifyJobCompletion(taskId, code) {
        if (this.transport === "stream") {
            return this.notifyJobCompletionStream(taskId, code);
        }
        clog.debug("[RemoteJobConnector] Adding result", code, "of task", taskId);
        await new Promise((resolve, reject) => {
            this.rcl.sadd(taskId, code, function (err, reply) {
                err ? reject(err): resolve(reply);
            });
        });
        clog.debug("[RemoteJobConnector] Marking task", taskId, "as completed");
        return new Promise((resolve, reject) => {
            this.rcl.sadd(this.completedNotificationQueueKey, taskId, function (err, reply) {
                err ? reject(err): resolve(reply);
            });
        });
    }
}

module.exports = RemoteJobConnector;
