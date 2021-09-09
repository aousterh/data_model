DATA_DIR="../workload/trace/"

WORKLOAD_FILE_NAME="network_log_search_30.ndjson"

OUTPUT_DIR="elastic-output/"

OUTPUT_FILE_NAME="elastic_network_log_search_30_execution_times.csv"

QUERIES = ["search id.orig_h","search id.orig_h + sort ts + slice 1000", "analytics avg orig_bytes"]


AGGREGATION_FIELDS= {"search id.orig_h + count by id.resp_h": "id.resp_h",
                                                 "search id.orig_h + count by schema": "_path" }

