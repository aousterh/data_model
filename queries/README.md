# Queries
To better understand the pros/cons of existing data models and query languages, let's try issuing some queries in different systems. These are the queries we will consider:

1. Analytics query - count the total number of records with each distinct source IP.
2. Search query - find all records with IP 10.128.0.19, sort by timestamp, and return the first 5.
3. Data discovery query - count the number of records with each different schema.

For these examples, we use some sample data from [Zeek](https://zeek.org/). To execute the queries yourself, clone the [zq-sample-data repo](https://github.com/brimsec/zq-sample-data) in the same directory that this repository is in.

We suggest using our public AMI (ami-0b1647c06db4be14e) because all of these tools are already installed. Make sure to select an instance type with enough memory for the `jq` queries; we use m5.large.


## jq

First we try issuing these queries over NDJSON data, which supports schema-less heterogeneous data in the same file. We the sed-like tool [`jq`](https://stedolan.github.io/jq/). You will need to [download and install `jq`](https://stedolan.github.io/jq/download/) and also unzip the NDJSON data by running `gzip -d *` in the `zq-sample-data/zeek-ndjson` directory.

You can run these queries with `./jq_queries.sh` or by setting `NDJSON_PATH` (`export NDJSON_PATH=../../zq-sample-data/zeek-ndjson/*.ndjson`) and executing the queries below.

#### 1. Analytics query

`jq -c -s 'group_by(."id.orig_h")[] | length as $l | .[0] | .count = $l | {count,"id.orig_h"}' $NDJSON_PATH`

This query is easy to write, but inefficient because it must scan all JSON objects in their entirety until a matching id.orig_h field is found (or the end of the record is reached).


#### 2. Search query

`jq -c -s '[ .[] | select(.["id.orig_h"]=="10.128.0.19") ] | sort_by(.ts) | .[:5] | .[]' $NDJSON_PATH`

This query is easy to write. It is inefficient, but you could issue a similar query using an index (e.g., with Elastic Search), and then it would be efficient.


#### 3. Data discovery query

`jq -c -s 'group_by(."_path")[] | length as $l | .[0] | .count = $l | {count,"_path"}' $NDJSON_PATH`

In JSON we have no schema information by default, so we use the "_path" field as a proxy for schema instead. This query is inefficient because we have to scan all records, as in the analytics example above.


## Spark (with Scala)

Next we try issuing these queries over [Spark](https://spark.apache.org/), using Scala. In Spark, data is stored in dataframes, typically with one type of data per dataframe. These queries involve more steps to run, so we only summarize the key parts of the code here. To run the full queries yourself, download Spark in the same directory as this repository (or use our public AMI which has Spark set up already) and execute:

`../../spark/bin/spark-shell -i spark_queries.scala`

#### 1. Analytics query

```
val matching_dfs = for {
    df <- dfs
    if df.columns.contains("id.orig_h")
} yield df

matching_dfs.map(df => df.select("`id.orig_h`")).reduce(_.union(_)).groupBy("`id.orig_h`").count().show()
```
In this query, we assume an array of dataframes, `df`, with one dataframe per schema of data. We first enumerate the dataframes that contain the `id.orig_h` column, then union the `id.orig_h` column from these dataframes and execute the query. This query is pretty easy to write and is much more efficient than the JSON, because (1) we skipped dataframes that didn't include the `id.orig_h` column and (2) the select could quickly extract all `id.orig_h` fields, leveraging the columnar format (which requires schema information).

#### 2. Search query

```
val all_columns = renamed_dfs.map(df => df.columns.toSet).reduce(_ ++ _)

def customSelect(availableCols: Set[String], requiredCols: Set[String]) = {
    requiredCols.toList.map(column => column match {
        case column if availableCols.contains(column) => col(column)
        case _ => lit(null).as(column)
    })
}

renamed_dfs.map(df => df.select(customSelect(df.columns.toSet, all_columns):_*)
    .filter(col("id_orig_h") === "10.128.0.19"))
    .reduce(_.union(_))
    .orderBy("ts")
    .limit(5)
    .toDF()
    .show()
```
This query was much harder to execute! First, we had to rename some columns that contained "." in them to avoid Spark errors (not shown). Spark is schema-rigid, so it doesn't let you process different schemas together. As a result, we next had to create an "uber schema" (`all_columns`) that contains all columns across all dataframes, so that we could pretend that this data all has the same schema. Then we had to fill in null values for missing columns (with the `customSelect` function). This query is probably also inefficient, because without indexes, Spark has to search the columns, and then reconstruct records from columnar data.


#### 3. Data discovery query

`dfs.map(df => (df.schema, df.count))`

The query above successfully returns an array of (schema, count) tuples. However, what if we wanted to put these results in a dataframe, with column names "schema" and "count", as we have with all other Spark query results so far?

`dfs.map(df => (df.schema, df.count)).toSeq.toDF("schema", "count")`

Unfortunately this throws a `scala.MatchError`, because you can't store schemas in Spark dataframes themselves. So, whenever you want to issue a query that returns a schema or type, you have to represent it using a different data structure (not a dataframe).


## Spark (with Python)

TODO


## Elasticsearch

Next we issue the search query using [Elasticsearch](https://www.elastic.co/) so that we can leverage its indexes for more efficient queries.

#### 2. Search query

TODO


## ZNG
Finally, we issue these queries over [ZNG](https://github.com/brimsec/zq/blob/master/zng/docs/spec.md) data. Follow the instructions [here](https://github.com/brimsec/zq) to install the command line query tool for ZNG, `zq`. As above, you need to unzip the ZNG data by running `gzip -d *` in the `zq-sample-data/zng-uncompressed` directory.

You can run these queries with `./zng_queries.sh` or by setting `ZNG_UNCOMPRESSED_PATH` (`export ZNG_UNCOMPRESSED_PATH=../../zq-sample-data/zng-uncompressed/*.zng`) and executing the queries below.

#### 1. Analytics query

`zq -t 'count() by id.orig_h' $ZNG_UNCOMPRESSED_PATH`

This query is easy to write and will be efficient once we can issue it over the columnar ZST format (this is not yet fully supported, so this query executes less efficiently over ZNG). This is possible because ZNG/ZST data is typed, thereby enabling efficient columnar representations.


#### 2. Search query

`zq -t 'id.orig_h=10.128.0.19 | sort ts | head 5' $ZNG_UNCOMPRESSED_PATH`

This query is easy to write because ZNG can represent heterogeneous records in the same stream, so you don't need to manually construct an uber-schema in order to represent the results of this query. However, this query is slow to execute over ZNG, because it must scan all records. Instead, let's build an index for the field `id.orig_h` using the tool `zar` and issue the query over the index (`export ZNG_PATH=../../zq-sample-data/zng/*.gz`).

```
mkdir logs
export ZAR_ROOT=`pwd`/logs
zq $ZNG_PATH | zar import -s 25MB -
zar index id.orig_h
zar zq -t 'id.orig_h=10.128.0.19 | sort ts | head 5'
```
That query was much faster! And, to keep things simple, zar represents the index itself in ZNG.


#### 3. Data discovery query

`zq -t 'count() by typeof(.)' $ZNG_UNCOMPRESSED_PATH`

This query returns a stream of (typeof, count) records, where "typeof" is a type and "count" is a uint64. This is possible because the query language supports first-class types (with typeof) and the data model supports first-class types, enabling the resulting ZNG stream to include values that are themselves types.
