#!/usr/bin/env python3

# run with taskset to restrict to one core!
# taskset -c 1 ./bench_type_contexts.py

from util import unix_time, unix_time_bash
import os
import random
import string
from pyspark.sql.functions import sum
from pyspark.sql.session import SparkSession

US_PER_S = 1000 * 1000
n_records = 1000 * 1000
factor = 8
results_dir = "type_context_results"

def parquet_dir(n_types):
    return "{}/parquet_{:0>7}_types".format(results_dir, n_types)

# query, query description
zed_queries = [("by typeof(.) | count()", "by typeof count"),
           ("count()", "count"),
           ("sum(value)", "sum")]
zed_formats = ["zng", "zst"]

# fuse exhausts all the memory with about 4,000 types
max_fused_types = 3 * 1000

def create_zed_data():
    type_range = []
    n = n_records
    while n > 1:
        type_range.insert(0, n)
        n = int(n / factor)
    type_range.insert(0, 1)
    print("types: " + str(type_range))

    # info about zed data (num types, base file name)
    zed_file_info = []

    # ZSON file handles
    zson_files = {}

    # open ZSON files
    for n_types in type_range:
        base_name = results_dir + "/data_{:0>7}_types".format(n_types)
        zed_file_info.append((n_types, base_name))
        zson_files[base_name] = open(base_name + ".zson", "w")

    # generate data as ZSON - records in different files have the same values
    # but vary field names to create different numbers of types per file
    for i in range(n_records):
        letter = random.choice(string.ascii_letters)
        value = random.randint(0, 1000)

        for n_types, base_name in zed_file_info:
            if n_types == 1:
                type_name = 0
            else:
                type_name = i % n_types

            f = zson_files[base_name]
            f.write("{{\"{:0>6}\":\"{}\",value:{}}}\n".format(type_name, letter, value))

    for f in zson_files.values():
        f.close()

    # create ZNG and ZST files from ZSON
    for n_types, base_name in zed_file_info:
        for zed_format in zed_formats:
            # regular version
            file_name = "{}.{}".format(base_name, zed_format)
            os.system("zq -f " + zed_format + " -o " + file_name + " " + base_name + ".zson")

            if n_types > max_fused_types:
                continue

            # fused version
            file_name = "{}_fused.{}".format(base_name, zed_format)
            os.system("zq -f " + zed_format + " -o " + file_name + " 'fuse' " + base_name + ".zson")

    return zed_file_info

def bench_zed(zed_file_info):
    zed_formats_expanded = []
    for zed_format in zed_formats:
        zed_formats_expanded.append((zed_format, "{}.{}", False))
        zed_formats_expanded.append((zed_format, "{}_fused.{}", True))

    # benchmark some queries over each file
    with open(results_dir + "/results.csv", "a") as f_results:
        for n_types, base_name in zed_file_info:

            for zed_format, file_str, fused in zed_formats_expanded:
                if fused and n_types > max_fused_types:
                    continue

                file_name = file_str.format(base_name, zed_format)

                # issue queries
                for query, description in zed_queries:
                    results = unix_time_bash("zq -i " + zed_format +
                                             " -Z \"" + query + "\" " +
                                             file_name)
                    format_str = zed_format + "_fused" if fused else zed_format
                    fields = [format_str, description, n_types, results['real'],
                              results['user'], results['sys'],
                              "{:.2f}".format(results['real'] * US_PER_S / n_types)]
                    f_results.write(",".join(str(x) for x in fields) + "\n")

def create_parquet_data(zed_file_info):
    spark = SparkSession.builder.master("local[1]").config("spark.executor.memory", "4g").getOrCreate()

    # convert ZNG to NDJSON
    for n_types, base_name in zed_file_info:
        os.system("zq -f ndjson -o {}.ndjson {}.zng".format(base_name, base_name))

    # convert NDJSON to Parquet
    for n_types, base_name in zed_file_info:
        # create directory for data with this many types
        os.system("rm -fr {}".format(parquet_dir(n_types)))
        os.system("mkdir {}".format(parquet_dir(n_types)))

        # merges all types into a single uber schema
        df = spark.read.json(base_name + ".ndjson")
        df.write.format("parquet").mode("append").save(parquet_dir(n_types))

def query_count(df):
    df.count()

def query_sum(df):
    df.select(sum("value")).show()

spark_queries = [(query_count, "count"),
                 (query_sum, "sum")]

def bench_parquet(zed_file_info):
    spark = SparkSession.builder.master("local[1]").config("spark.executor.memory", "4g").getOrCreate()

    with open(results_dir + "/results.csv", "a") as f_results:
        for n_types, base_name in zed_file_info:
            df = spark.read.parquet("{}/*.parquet".format(parquet_dir(n_types)))

            for query, description in spark_queries:
                results = unix_time(query, df=df)

                results_str = {}
                for k in ["real", "user", "sys"]:
                    results_str[k] = "{:.2f}".format(results[k])

                fields = ["parquet", description, n_types, results_str['real'],
                          results_str['user'], results_str['sys'],
                          "{:.2f}".format(results['real'] * US_PER_S / n_types)]
                f_results.write(",".join(str(x) for x in fields) + "\n")

def main():
    os.system("rm -fr " + results_dir)
    os.system("mkdir " + results_dir)

    with open(results_dir + "/results.csv", "w") as f_results:
        f_results.write("format,query,n_types,real,user,sys,us_per_type\n")

    zed_file_info = create_zed_data()
    bench_zed(zed_file_info)

    zed_file_info_subset = []
    for n_types, base_name in zed_file_info:
        if n_types < 5000:
            zed_file_info_subset.append((n_types, base_name))
    print("parquet subset: " + str(zed_file_info_subset))
    create_parquet_data(zed_file_info_subset)
    bench_parquet(zed_file_info_subset)

if __name__ == '__main__':
    main()
