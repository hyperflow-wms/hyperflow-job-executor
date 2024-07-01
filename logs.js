const {Resource} = require('@opentelemetry/resources')
const {
    LoggerProvider,
    SimpleLogRecordProcessor
} = require('@opentelemetry/sdk-logs');
const {SemanticResourceAttributes} = require('@opentelemetry/semantic-conventions')
const { OTLPLogExporter } = require('@opentelemetry/exporter-logs-otlp-http');


module.exports = (serviceName) => {

    const resource = new Resource({
        [ SemanticResourceAttributes.SERVICE_NAME ]: serviceName
    });

    const loggerProvider = new LoggerProvider({
        resource
    });

    loggerProvider.addLogRecordProcessor(new SimpleLogRecordProcessor(new OTLPLogExporter({
        url: process.env.HF_VAR_OPT_URL + ':4318/v1/logs',
        keepAlive: true,
    })));

    return loggerProvider.getLogger('default');
}