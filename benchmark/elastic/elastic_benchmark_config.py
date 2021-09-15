DATA_DIR="../workload/trace/"

#WORKLOAD_FILE_NAME="network_log_search_needles_30.ndjson"
#WORKLOAD_FILE_NAME="network_log_search_needles_30_2.ndjson"
WORKLOAD_FILE_NAME="network_log_analytics_avg_30.ndjson"
#WORKLOAD_FILE_NAME="debug.ndjson"

OUTPUT_DIR="elastic-output/"

#OUTPUT_FILE_NAME="elastic_network_log_search_needles_30_execution_times.csv"
#OUTPUT_FILE_NAME="elastic_network_log_search_needles_30_2_execution_times.csv"
OUTPUT_FILE_NAME="elastic_network_log_analytics_avg_30_execution_times.csv"
#OUTPUT_FILE_NAME="debug.csv"

QUERIES = ["search id.orig_h","search id.orig_h + sort ts + slice 1000", "analytics avg field"]


AGGREGATION_FIELDS= {"search id.orig_h + count by id.resp_h": "id.resp_h",
                                                 "search id.orig_h + count by schema": "_path" }
