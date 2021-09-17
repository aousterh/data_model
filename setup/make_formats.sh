# WARNING: this not a script that you can just run. It's a bunch of notes about
# commands that can be used to generate all of the different formats of data,
# but it will not necessarily just run all by itself, it requires babysitting
# and probably misses some steps.

# TODO: make this a proper script
# TODO: some steps below could probably be combined/simplified

BASE_DIR=subset_80_million


mkdir -p $BASE_DIR/z/zng

# assumes you already unzipped... but really should just combine these.
# doesn't zip the ZNG again.
# write all the data to ZNG, then load to a lake for easier subsetting (could maybe combine?)
for file in zeek-default/*
do
    $ZQ -f zng "$file" > z/zng/"$(basename "$file" | sed 's/\.log//')".zng
done


# once you have a subset of data in all.zng...

# convert it to ndjson and to zeek
time zq -f ndjson $BASE_DIR/z/all.zng > $BASE_DIR/all.ndjson &
time zq -f zeek $BASE_DIR/z/all.zng > $BASE_DIR/all.log

# get the time range so that you can extract the same data from the JSON streaming data
time zq -validate=f -Z "min(ts),max(ts)" $BASE_DIR/z/all.zng

JSON_STREAMING_DIR=/zeek-data-one-tb/zeek-ndjson

# note that this is a string sort comparison rather than by timestamp because
# JSON doesn't have proper timestamps
# took about 6 hours for conn - maybe split this up next time
for file in $JSON_STREAMING_DIR/*
do
    time zq -f ndjson "ts >= \"2018-03-24T19:16:02.320478Z\"" "$file" \
       > $BASE_DIR/zeek-ndjson/"$(basename "$file" | sed 's/\.log\.gz//')".ndjson &
done

# check that the count is right or close
# took 52 mins
time zq -Z "count()" $BASE_DIR/zeek-ndjson/json_streaming_*

# use ZNG to preprocess the data to make it easier to generate workloads
# only 34 types because conn is twice and there are no loaded_scripts types
time zq -f ndjson -validate=false "sample | put .:= sample" $BASE_DIR/z/all.zng > $BASE_DIR/type_samples.ndjson
time zq -f ndjson -validate=false "cut id.orig_h | sort | uniq | put .:=id" $BASE_DIR/z/all.zng > $BASE_DIR/id_orig_h.ndjson


# generate one nested ndjson file per type, to use to create Parquet data
# timestamps are not strings here!
# timestamps were chosen for the subset of 80 million records
mkdir $BASE_DIR/ndjson-nested
for file in zng/*
do
    time zq -f ndjson "ts >= 2018-03-24T19:16:02.320478Z" "$file" \
	 > $BASE_DIR/ndjson-nested/"$(basename "$file" | sed 's/\.zng//')".ndjson &
done

# create Parquet by running in the data_model/setup/spark directory:
time python ./ndjson_to_parquet_via_spark.py

# split neseted ndjson into chunks
mkdir $BASE_DIR/ndjson-nested-chunks
for file in $BASE_DIR/ndjson-nested/*
do
    time split -l 1000000 --additional-suffix=".ndjson" "$file" $BASE_DIR/ndjson-nested-chunks/"$(basename "$file" | sed 's/\.ndjson//')"
done


# fuse zeek data to avoid issues with Postgres
# took over an hour
ZEEK_DIR=/zeek-data-one-tb/subset_80_million/zeek-ndjson
mkdir $BASE_DIR/zeek-ndjson-fused
for file in $ZEEK_DIR/*
do
    time zq -f ndjson "fuse" $file > $BASE_DIR/zeek-ndjson-fused/"$(basename "$file")" &
done


# split flattened fused ndjson into chunks.
# keep these small to avoid using too much memory when loading into postgres!
mkdir $BASE_DIR/zeek-ndjson-fused-chunks
for file in $BASE_DIR/zeek-ndjson-fused/*
do
    time split -l 10000 --additional-suffix=".ndjson" "$file" $BASE_DIR/zeek-ndjson-fused-chunks/"$(basename "$file" | sed 's/\.ndjson//')"
done
