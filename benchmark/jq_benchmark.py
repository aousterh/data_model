#!/usr/bin/env python3
from util import benchmark_bash

# assume volume is mounted at /zq-sample-data
BASE_DIR = "/zq-sample-data"
DATA = BASE_DIR + "/z"
DATA_NDJSON = BASE_DIR + "/ndjson/zeek-ndjson"
DATA_NDJSON_NESTED = BASE_DIR + "/ndjson-nested"

jq_query = "zcat {} | jq -c -s '{}'"

# specific jq filters
analytics = 'group_by(."id.orig_h")[] | {"count": length, "id.orig_h": .[0]."id.orig_h"}'
search = '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | .[]'
search_sort = '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | sort_by(.ts) | .[]'
search_sort_top_5 = '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | sort_by(.ts) | .[:5] | .[]'
discovery = 'group_by(."_path")[] | {"count": length, "_path": .[0]."_path"}'

all_queries = [analytics, search, search_sort, search_sort_top_5, discovery]

def main():
    print("name,real,user,sys")
    print("------------------")

    print("ndjson flat")
    for q in all_queries:
        benchmark_bash(jq_query.format(DATA_NDJSON + "/*.gz", q))

    print("ndjson nested")
    for q in all_queries:
        benchmark_bash(jq_query.format(DATA_NDJSON_NESTED + "/*.gz", q))

if __name__ == '__main__':
    main()
