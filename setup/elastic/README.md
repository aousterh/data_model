Bulk Loading NDJSONs into ElasticSearch Instructions:
1. Run make-bulk-format in directory with ndjsons
2. cd  to ../bulk-format 
3. Run bulk-load
4. Run curl -X GET "localhost:9200/test/_count?pretty' to check number of records in ElasticSearch for confirmation
