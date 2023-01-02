/**
 * Class for notifying HyperFlow's master about tasks' results.
 *
 * Details:
 *  1) exit code is added into set 'wf:[wfID]:completedTasks',
 *   where [wfID] is workflow ID,
 *  2) then task completion is marked by pushing 'code' into 'taskId' set.
 */
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
    }

    /**
     * Notify HyperFlow about remote job completion.
     * @param {string} taskId task ID
     * @param {number} code exit code
     */
    async notifyJobCompletion(taskId, code) {
        console.log("[RemoteJobConnector] Adding result", code, "of task", taskId);
        await new Promise((resolve, reject) => {
            this.rcl.sadd(taskId, code, function (err, reply) {
                err ? reject(err): resolve(reply);
            });
        });
        console.log("[RemoteJobConnector] Marking task", taskId, "as completed");
        return new Promise((resolve, reject) => {
            this.rcl.sadd(this.completedNotificationQueueKey, taskId, function (err, reply) {
                err ? reject(err): resolve(reply);
            });
        });
    }
}

module.exports = RemoteJobConnector;
