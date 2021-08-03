DATA_DIR="workload/trace/test/"

WORKLOAD_FILE_NAME="network_log_search_30.ndjson"

OUTPUT_DIR="elastic-output/"

OUTPUT_FILE_NAME="query_execution_times.csv"

QUERIES = ["search id.orig_h", 
           "search id.orig_h + sort ts", 
           "search id.orig_h + sort ts + slice 5",
           "search id.orig_h + count by id.resp_h",
           "search id.orig_h + sum orig_bytes",
           "search id.orig_h + count by schema"]


AGGREGATION_FIELDS= {"search id.orig_h + count by id.resp_h": "id.resp_h", 
					 "search id.orig_h + count by schema": "_path" }

