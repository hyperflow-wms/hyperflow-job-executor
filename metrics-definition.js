const meter = process.env.HF_VAR_ENABLE_TRACING === "1" ? require("./metrics.js")("hyperflow-job-executor"): undefined;


const cpuMetric = meter.createObservableGauge('cpu-usage', {
    description: 'CPU usage',
    unit: 'Percentage'
});

const memoryMetric = meter.createObservableGauge('memory-usage', {
    description: 'Memory usage',
    unit: 'Bytes'
});

const cTimeMetric = meter.createObservableGauge('ctime', {
    description: 'cTime',
    unit: 'ms'
});


// Export constants using module.exports
module.exports = {
    cpuMetric: cpuMetric,
    memoryMetric: memoryMetric,
    cTimeMetric: cTimeMetric,
};