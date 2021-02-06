# Queries
To better understand the pros/cons of existing data models and query languages, let's try issuing some queries in different systems. These are the queries we will consider:

1. Analytics query - count the total number of records with each distinct source IP.
2. Search query - find all records with IP 10.128.0.19, sort by timestamp, and return the first 5.
3. Data discovery query - count the number of records with each different schema.

For these examples, we use some sample data from [Zeek](https://zeek.org/). To execute the queries yourself, clone the [zq-sample-data repo](https://github.com/brimsec/zq-sample-data) in the same directory that this repository is in.


## jq

First we try issuing these queries over NDJSON data, using the sed-like tool [`jq`](https://stedolan.github.io/jq/). You will need to [download and install `jq`](https://stedolan.github.io/jq/download/) and also unzip the NDJSON data by running `gzip -d *` in the `zq-sample-data/zeek-ndjson` directory.

You can run these queries with `./jq_queries.sh` or by setting `NDJSON_PATH` (`export NDJSON_PATH=../../zq-sample-data/zeek-ndjson/*.ndjson`) and executing the queries below.

### 1. Analytics query

`jq -c -s 'group_by(."id.orig_h")[] | length as $l | .[0] | .count = $l | {count,"id.orig_h"}' $NDJSON_PATH`

This query is easy to write, but inefficient because it must scan all JSON objects in their entirety until a matching id.orig_h field is found (or the end of the record is reached).


### 2. Search query

`jq -c -s '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | sort_by(.ts) | .[:5] | .[]' $NDJSON_PATH`

This query is easy to write. It is inefficient, but you could issue a similar query using an index (e.g., with Elastic Search), and then it would be efficient.


### 3. Data discovery query

`jq -c -s 'group_by(."_path")[] | length as $l | .[0] | .count = $l | {count,"_path"}' $NDJSON_PATH`

In JSON we have no schema information by default, so we use the "_path" field as a proxy for schema instead. This query is inefficient because we have to scan all records, as in the analytics example above.