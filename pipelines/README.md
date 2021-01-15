# Pipelines
Scripts for setting up different data processing pipelines.

## No Schema

Sources generate JSON and send it to Kafka. Then search and analytics pipelines consume data from Kafka.

### Search cluster

Elastic Search
1. Run make-bulk-format in directory with ndjsons
2. Transfer *.new files to a new directory
3. Run rename 's/.new//' * or similar command to remove .new extension added by make-bulk-format script
4. Start elastic search with sudo systemctl start elasticsearch.service or bin/elasticsearch
5. Verify elastic search is running by running curl -X GET http://localhost:9200/ 
5. Run bulk-load to load into elastic search

### Analytics cluster

Spark over Parquet files

## Rigid Schema

## ZNG/ZST
