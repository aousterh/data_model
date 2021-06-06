#!/bin/bash
# shellcheck disable=SC2016    # The backticks in quotes are for markdown, not expansion
# based off of https://github.com/brimsec/zq/blob/master/scripts/comparison-test


set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# assume volume is mounted at /zq-sample-data
BASE_DIR="/zq-sample-data"
DATA=$BASE_DIR"/z"
DATA_NDJSON=$BASE_DIR"/ndjson/zeek-ndjson"
DATA_NDJSON_NESTED=$BASE_DIR"/ndjson-nested"

if [[ $(type -P "gzcat") ]]; then
  ZCAT="gzcat"
elif [[ $(type -P "zcat") ]]; then
  ZCAT="zcat"
else
  echo "gzcat/zcat not found in PATH"
  exit 1
fi

for CMD in zq jq; do
  if ! [[ $(type -P "$CMD") ]]; then
    echo "$CMD not found in PATH"
    exit 1
  fi
done

declare -a MARKDOWNS=(
#    '01_all_unmodified.md'
#    '02_cut_ts.md'
#    '03_count_all.md'
    '04_count_by_id_orig_h.md'
#    '05_only_id_resp_h.md'
    '06_search_top_5.md'
    '07_count_by_type.md'
    '08_search.md'
    '09_search_sort.md'
)

declare -a DESCRIPTIONS=(
#    'Output all events unmodified'
#    'Extract the field `ts`'
#    'Count all events'
    'Count all events, grouped by the field `id.orig_h`'
#    'Output all events with the field `id.resp_h` set to `52.85.83.116`'
    'Top 5 events with `id.orig_h` set to `10.128.0.19`'
    'Count all events, grouped by type'
    'All events with `id.orig_h` set to `10.128.0.19`'
    'All events with `id.orig_h` set to `10.128.0.19`, sorted'
)

declare -a ZQL_QUERIES=(
#    '*'
#    'cut ts'
#    'count()'
    'count() by id.orig_h'
#    'id.resp_h=52.85.83.116'
    'id.orig_h=10.128.0.19 | sort ts | head 5'
    'count() by typeof(.)'
    'id.orig_h=10.128.0.19'
    'id.orig_h=10.128.0.19 | sort ts'
)

declare -a JQ_FILTERS=(
#    '.'
#    '. | { ts }'
#    '. | length'
    'group_by(."id.orig_h")[] | {"count": length, "id.orig_h": .[0]."id.orig_h"}'
#    '. | select(.["id.resp_h"]=="52.85.83.116")'
    '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | sort_by(.ts) | .[:5] | .[]'
    'group_by(."_path")[] | {"count": length, "_path": .[0]."_path"}'
    '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | .[]'
    '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | sort_by(.ts) | .[]'
)

declare -a JQFLAGS=(
#    '-c'
#    '-c'
#    '-c -s'
    '-c -s'
#    '-c'
    '-c -s'
    '-c -s'
    '-c -s'
    '-c -s'
)

#INPUT_FORMATS=(zng zng-uncompressed zson tzng ndjson)
#OUTPUT_FORMATS=(zng zng-uncompressed zson tzng ndjson)
INPUT_FORMATS=(zng zng-uncompressed zst)
OUTPUT_FORMATS=(zng zng-uncompressed zst)
for (( n=0; n<"${#ZQL_QUERIES[@]}"; n++ ))
do
    DESC=${DESCRIPTIONS[$n]}
    MD=${MARKDOWNS[$n]}
    zql=${ZQL_QUERIES[$n]}
    echo -e "### $DESC\n" | tee "$MD"
    echo "|**<br>Tool**|**<br>Arguments**|**Input<br>Format**|**Output<br>Format**|**<br>Real**|**<br>User**|**<br>Sys**|" | tee -a "$MD"
    echo "|:----------:|:---------------:|:-----------------:|:------------------:|-----------:|-----------:|----------:|" | tee -a "$MD"
    for INPUT in ${INPUT_FORMATS[@]} ; do
      for OUTPUT in ${OUTPUT_FORMATS[@]} ; do
        echo -n "|\`zq\`|\`$zql\`|$INPUT|$OUTPUT|" | tee -a "$MD"
        if [[ $INPUT == "zng-uncompressed" ]]; then
          INPUT_FMT="zng"
        else
          INPUT_FMT="$INPUT"
        fi
        if [[ $OUTPUT == "zng-uncompressed" ]]; then
          ALL_TIMES=$(time -p (zq -i "$INPUT_FMT" -f zng -znglz4blocksize 0 "$zql" $DATA/$INPUT/* > /dev/null) 2>&1)
        else
          ALL_TIMES=$(time -p (zq -i "$INPUT_FMT" -f "$OUTPUT" "$zql" $DATA/$INPUT/* > /dev/null) 2>&1)
        fi
        echo "$ALL_TIMES" | tr '\n' ' ' | awk '{ print $2 "|" $4 "|" $6 "|" }' | tee -a "$MD"
      done
    done

    for NDJSON_DATA in $DATA_NDJSON $DATA_NDJSON_NESTED; do
	if [[ $NDJSON_DATA == $DATA_NDJSON ]]; then
	    FMT="ndjson flat"
	else
	    FMT="ndjson nested"
	fi
	
        JQ=${JQ_FILTERS[$n]}
        JQFLAG=${JQFLAGS[$n]}
        echo -n "|\`jq\`|\`$JQFLAG ""'""${JQ//|/\\|}""'""\`|$FMT|$FMT|" | tee -a "$MD"
        # shellcheck disable=SC2086      # For expanding JQFLAG
        ALL_TIMES=$(time -p ($ZCAT "$NDJSON_DATA"/*.gz | jq $JQFLAG "$JQ" > /dev/null) 2>&1)
        echo "$ALL_TIMES" | tr '\n' ' ' | awk '{ print $2 "|" $4 "|" $6 "|" }' | tee -a "$MD"
    done

    echo | tee -a "$MD"
done
