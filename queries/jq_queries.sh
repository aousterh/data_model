#!/bin/bash

NDJSON_PATH=../../zq-sample-data/zeek-ndjson/*.ndjson


printf "Analytics query\n"
printf "count total number of records with each distinct source IP\n"
jq -c -s 'group_by(."id.orig_h")[] | length as $l | .[0] | .count = $l | {count,"id.orig_h"}' $NDJSON_PATH

printf "\nSearch query\n"
printf "find all records with IP 10.128.0.19, sort by timestamp, and return the first 5\n"
jq -c -s '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | sort_by(.ts) | .[:5] | .[]' $NDJSON_PATH

printf "\nData discovery query\n"
printf "count the number of records with each different schema\n"
jq -c -s 'group_by(."_path")[] | length as $l | .[0] | .count = $l | {count,"_path"}' $NDJSON_PATH
