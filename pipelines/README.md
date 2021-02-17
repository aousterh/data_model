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

Here are the steps from the dataset to running queries:

convert ndjson to parquet
run cd data_model/pipelines/spark at root directory
run python3 ndj2par.py will produce all parquet files in data_model/parquet

create merged data frame
run cd data_model/pipelines/sparkto get to spark pipeline directory
run python3 merge_via_json.pywill produce a merged dataframe written in parquet format in data_model/merged

run spark queries over merged data frames
run cd data_model/queries to get to the queries directory
run python3 spark_queries.py to issue queries

## Rigid Schema

## ZNG/ZST
