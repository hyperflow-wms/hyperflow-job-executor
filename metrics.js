const {PeriodicExportingMetricReader, ConsoleMetricExporter, MeterProvider} = require('@opentelemetry/sdk-metrics')
const {Resource} = require('@opentelemetry/resources')
const {SemanticResourceAttributes} = require('@opentelemetry/semantic-conventions')
const {OTLPMetricExporter} = require('@opentelemetry/exporter-metrics-otlp-http')


module.exports = (serviceName) => {

    const exporter = new OTLPMetricExporter({
        url: process.env.HF_VAR_OPT_URL+':4318/v1/metrics'
    })
    const metricReader = new PeriodicExportingMetricReader(
        {exporter: exporter,
            exportIntervalMillis: 2000}
    )
    return new MeterProvider({
        readers: [metricReader],
        resource: new Resource({
            [SemanticResourceAttributes.SERVICE_NAME]:
            serviceName,
        }),
    });
}