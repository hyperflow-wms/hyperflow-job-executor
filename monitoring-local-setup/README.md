# HyperFlow Job executor monitoring

## Set required env variables to
- HF_VAR_ENABLE_TRACING: 1
- HF_VAR_OPT_URL: http://localhost

## Run docker compose
docker compose up -d

## Check if all containers work
Opensearch might fail to start due to low memory, if it happens check:
https://stackoverflow.com/questions/51445846/elasticsearch-max-virtual-memory-areas-vm-max-map-count-65530-is-too-low-inc

Verify if dataprepper properly initialized connection with sink (opensearch) by checking logs

## Run workflow locally

## Open opensearch dashboards

Navigate to
http://localhost:5601/

login using credentials 
admin
Hyperflow1!

Go to Dashboards Management -> Index Patterns

create index patterns
- hyperflow_traces
- hyperflow_metrics
- hyperflow_logs

Go to Discover and choose one of new index patterns as source
