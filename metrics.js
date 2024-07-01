const {PeriodicExportingMetricReader, ConsoleMetricExporter, MeterProvider} = require('@opentelemetry/sdk-metrics')
const {Resource} = require('@opentelemetry/resources')
const {SemanticResourceAttributes} = require('@opentelemetry/semantic-conventions')
const {OTLPMetricExporter} = require('@opentelemetry/exporter-metrics-otlp-http')


module.exports = (serviceName) => {

    const exporter = new OTLPMetricExporter({
        url: process.env.HF_VAR_OPT_URL+':4318/v1/metrics'
    })
    const consoleReader = new PeriodicExportingMetricReader(
        {exporter: exporter,
            exportIntervalMillis: 3000}
    )
    const provider = new MeterProvider({
        readers: [consoleReader],
        resource: new Resource({
            [SemanticResourceAttributes.SERVICE_NAME]:
            serviceName,
        }),
    });

    return provider.getMeter("hyperflow-job-executor");
}