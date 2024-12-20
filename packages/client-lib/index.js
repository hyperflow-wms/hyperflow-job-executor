const redis = require('redis');
const RedisConnector = require('./redisConnector');
const EventEmitter = require('events');

class TaskClient extends EventEmitter {
    constructor(workId, redisURL = 'redis://127.0.0.1:6379') {
        super();
        this.workId = workId;
        this.tasks = new Map();
        this.rcl = redis.createClient(redisURL);
        this.rcl.on('error', (err) => console.error('Redis Client Error:', err));

        (async () => {
            try {
                await this.rcl.connect();
            } catch (error) {
                console.error('failed to connect to redis:', error);
            }
        })();

        this.connector = new RedisConnector(this.rcl, workId, 3000);
        this.connector.run();
    }

    async notifyTaskCompletion(taskId, code) {
        return this.connector.notifyTaskCompletion(taskId, code);
    }

    /**
     * Send a task to the executor framework
     * @param {String} taskId - Globally unique task identifier
     * @param {Object} taskData - The data required to execute the task on the server
     * @returns {Promise<string>} - A promise that resolves with the task ID
     * 
     * TODO: implement queuing tasks via RABBITMQ
     */
    async sendTaskData(taskId, taskData) {
        const rcl = this.rcl;
        const taskMessageKey = taskId + "_msg";
        return rcl.lPush(taskMessageKey, taskData);
    }

    async getTaskData(taskId, timeout = 3000) {
        const taskDataKey = taskId + "_msg";
        return this.rcl.brPopLPush(taskDataKey, taskDataKey, timeout);
    }

    /*async sendTask(taskData) {
        try {
            if (getExecutorType(context) === "WORKER_POOL") { // FIXME: get via configuration
                await amqpEnqueueJobs(jobArr, taskIdArr, contextArr, customParams)
            } else {
                await submitK8sJob(kubeconfig, jobArr, taskIdArr, contextArr, customParams)
            }
    }*/

    /**
     * Wait for a task to complete and return its result (exit code)
     * @param {string} taskId - The ID of the task to wait for
     * @returns {Promise<Object>} - A promise that resolves with the task result or rejects on failure
     */
    async waitAndGetResult(taskId, timeout = 1000) {
        return this.connector.waitForTask(taskId);
    }

    /**
     * Simulate task submission and waiting in one go
     * @param {string} taskId - The ID of the task to wait for
     * @param {Object} taskData - The data required to execute the task
     * @returns {Promise<Object>} - A promise that resolves with the task result
     */
    async sendDataAndWaitForTask(taskId, taskData) {
        await this.sendTaskData(taskId, taskData);
        return this.waitAndGetResult(taskId);
    }
}


module.exports = TaskClient;

// Simple tests
if (require.main === module) {
    const taskId = "myTaskId";
    const client = new TaskClient("myWorkId1");
    const assert = require('assert');
    var code;

    async function simpleTest() {
        try {
            await client.sendTaskData(taskId, "task data");
            await client.notifyTaskCompletion(taskId, "0");
            //console.log(client.connector.completedNotificationQueueKey);
            //let setmembers = await client.rcl.sMembers(client.connector.completedNotificationQueueKey);
            //console.log(setmembers);
            const taskData = await client.getTaskData(taskId);
            console.log("Task data", taskData);
            code = await client.waitAndGetResult(taskId);
            console.log("task exit code:", code);
        } catch (error) {
            console.error('failed to connect to redis:', error);
        }
    }


    async function testComplexTaskFlow(client, taskCount = 100) {
        function generateUniqueId() {
            return Date.now().toString(36) + Math.random().toString(36).slice(2, 5);
        }

        const taskIds = Array.from({ length: taskCount }, () => generateUniqueId());
        const taskDatas = taskIds.map(id => id + "_data");
        const promises = [];
        console.log(taskIds, taskDatas);

        // Step 1: Send task data asynchronously
        taskIds.forEach((taskId, index) => {
            promises.push(client.sendTaskData(taskId, taskDatas[index]));
        });
        await Promise.all(promises); // Wait for all sendTaskData to complete
        console.log("All tasks data sent.");

        // Step 2: Invoke waitAndGetResult for all tasks simultaneously
        const resultPromises = taskIds.map(taskId => client.waitAndGetResult(taskId));

        // Step 3: Notify task completion in random order
        const shuffledTaskIds = [...taskIds].sort(() => Math.random() - 0.5); // Shuffle taskIds
        shuffledTaskIds.forEach((taskId, index) => {
            setTimeout(() => {
                client.notifyTaskCompletion(taskId, "0");
            }, Math.random() * 8000); // Random delay up to 8 seconds 
        });

        // Step 4: Wait for all waitAndGetResult to resolve
        const results = await Promise.all(resultPromises);

        // Step 5: Check if everything is okay
        results.forEach((code, index) => {
            assert.strictEqual(code[1], "0", `Exit code should be '0' for taskId ${taskIds[index]}`);
            console.log(`Task exit code for ${taskIds[index]}:`, code);
        });

        // Additional checks can be added here, e.g., verifying task data
        for (let i = 0; i < taskIds.length; i++) {
            const retrievedTaskData = await client.getTaskData(taskIds[i]);
            assert.strictEqual(retrievedTaskData, taskDatas[i], `Task data mismatch for taskId ${taskIds[i]}`);
            console.log(`Task data for ${taskIds[i]}:`, retrievedTaskData);
        }

        client.connector.stop();
        console.log('All tasks processed successfully.');
    }

    testComplexTaskFlow(client).catch(console.error);
}