#!/usr/bin/env python3

# run with taskset to restrict to one core!
# taskset -c 1 ./bench_type_contexts.py

from enum import Enum
from util import *
import glob
from itertools import product
import os
import random
import re
import string
from pyspark.sql.functions import sum
from pyspark.sql.session import SparkSession

US_PER_S = 1000 * 1000
n_records = 1000 * 1000
#max_types = n_records
max_types = 10 * 1000
factor = 8
results_dir = "type_context_results"
results_csv = "{}/results.csv".format(results_dir)
sizes_csv = "{}/sizes.csv".format(results_dir)

# query, query description
zed_queries = [#("by typeof(.) | count()", "by typeof count"),
               ("count()", "count"),
               ("sum(value)", "sum")]
zed_formats = ["zng"] #, "zst"]
all_formats = zed_formats + ["zson", "ndjson", "parquet"]

# fuse exhausts all the memory with about 4,000 types
max_zed_fused_types = max_types
max_parquet_fused_types = max_types

# enum for different ways of organizing data across files
class Org(Enum):
    DEFAULT = "default"
    FUSED = "fused" # uber-schema, fused version
    SILOED = "siloed" # single type per file

    def org_dir(self, n_types):
        return "{}/{}_{:0>7}_types".format(results_dir, self.name.lower(),
                                           n_types)

def type_name(index, n_types):
    return "f{:0>7}".format(index % n_types)

def default_file(n_types, file_format):
    return "{}/data.{}".format(Org.DEFAULT.org_dir(n_types), file_format)

def fused_file(n_types, file_format):
    return "{}/data.{}".format(Org.FUSED.org_dir(n_types), file_format)

def siloed_file(n_types, type_index, file_format):
    t_name = type_name(type_index, n_types)
    return "{}/{}.{}".format(Org.SILOED.org_dir(n_types), t_name, file_format)

def get_type_range():
    type_range = []
    n = max_types
    while n > 1:
        type_range.insert(0, n)
        n = int(n / factor)
    type_range.insert(0, 1)
    print("types: " + str(type_range))

    return type_range

def make_dirs(type_range):
    os.system("rm -fr " + results_dir)
    os.system("mkdir " + results_dir)

    # copy this file so we have a record of how this experiment was run
    os.system("cp " + str(__file__) + " " + results_dir)

    # create directories for fused and siloed organizations of data
    for n_types in type_range:
        for org in Org:
            os.system("mkdir {}".format(org.org_dir(n_types)))

def create_zed_data(type_range):
    print("creating zed data")
    # ZSON file handles
    zson_files = {}

    # open ZSON files
    for n_types in type_range:
        zson_files[n_types] = open(default_file(n_types, "zson"), "w")

    # generate data as ZSON - records in different files have the same values
    # but vary field names to create different numbers of types per file
    print("creating zed data (ZSON)")
    for i in range(n_records):
        letter = random.choice(string.ascii_letters)
        value = random.randint(0, 1000)

        for n_types in type_range:
            f = zson_files[n_types]
            f.write("{{{}:\"{}\",value:{}}}\n".format(type_name(i, n_types),
                                                      letter, value))

    for f in zson_files.values():
        f.close()

    # create ZNG, ZST, and NDJSON files from ZSON
    print("creating zed data (ZNG, ZST, NDJSON)")
    for n_types in type_range:
        for zed_format in zed_formats:
            # regular version
            os.system("zq -f {} -o {} {}".format(
                zed_format, default_file(n_types, zed_format), default_file(n_types, "zson")))

            if n_types > max_zed_fused_types:
                continue

            # fused version
            os.system("zq -f {} -o {} 'fuse' {}".format(
                zed_format, fused_file(n_types, zed_format), default_file(n_types, "zson")))

        # convert to NDJSON
        os.system("zq -f ndjson -o {} {}".format(
            default_file(n_types, "ndjson"), default_file(n_types, "zson")))

    # create files with a single type per file
    print("creating zed data (siloed)")
    for n_types in type_range:
        for i in range(n_types):
            for zed_format in zed_formats:
                input_file = default_file(n_types, zed_format)
                t_name = type_name(i, n_types)
                siloed_file_name = siloed_file(n_types, i, zed_format)
                os.system("zq -validate=false -i {} -f {} -o {} 'has({})' {}"
                          .format(zed_format, zed_format, siloed_file_name, t_name, input_file))

            # convert to NDJSON
            ndjson_file = siloed_file(n_types, i, "ndjson")
            os.system("zq -validate=false -f ndjson -o {} {}".format(
                ndjson_file, siloed_file(n_types, i, zed_formats[0])))

def bench_zed(type_range):
    print("benchmarking zed data")
    # benchmark some queries over each file
    with open(results_csv, "a") as f_results:
        for n_types in type_range:

            for zed_format, organization in product(zed_formats,
                                                    [org for org in Org]):
                if organization == Org.FUSED and n_types > max_zed_fused_types:
                    continue

                if organization == Org.SILOED:
                    # use * to match all siloed files
                    file_name = "{}/*.{}".format(organization.org_dir(n_types), zed_format)
                elif organization == Org.FUSED:
                    file_name = fused_file(n_types, zed_format)
                else:
                    file_name = default_file(n_types, zed_format)

                # issue queries
                for query, description in zed_queries:
                    flush_buffer_cache()

                    cmd = "zq -i {} -z -validate=false \"{}\" {}".format(zed_format, query, file_name)
                    print("running cmd: {}".format(cmd))
                    results = unix_time_bash(cmd, stdout=subprocess.PIPE)
                    return_val = re.search(r"{(.*):(\d*)", results['return']).groups()[1]
                    fields = [zed_format, organization.value, description,
                              n_types, results['real'], results['user'],
                              results['sys'],
                              "{:.2f}".format(results['real'] * US_PER_S / n_types),
                              return_val]
                    f_results.write(",".join(str(x) for x in fields) + "\n")

def create_parquet_data(type_range):
    print("creating parquet data")
    spark = SparkSession.builder.master("local[1]").config("spark.executor.memory", "4g").getOrCreate()

    # convert NDJSON to Parquet
    for n_types in type_range:
        print("creating parquet data with {} types".format(n_types))
        if n_types <= max_parquet_fused_types:
            # merges all types into a single fused schema
            df = spark.read.json(default_file(n_types, "ndjson"))
            df.write.format("parquet").mode("append").save(Org.FUSED.org_dir(n_types))

        # create siloed data
        for i in range(n_types):
            df = spark.read.json(siloed_file(n_types, i, "ndjson"))
            df.write.format("parquet").mode("append").save(Org.SILOED.org_dir(n_types))

def query_count(spark, paths):
    df = spark.read.parquet(paths[0])
    return df.count()

def query_count_siloed(spark, paths):
    count = 0
    for file_path in paths:
        df = spark.read.parquet(file_path)
        count += df.count()
    return count

def query_sum(spark, paths):
    df = spark.read.parquet(paths[0])
    return df.select(sum("value")).first()[0]

def query_sum_siloed(spark, paths):
    total = 0
    for file_path in paths:
        df = spark.read.parquet(file_path)
        total += df.select(sum("value")).first()[0]
    return total

# query and organization
spark_queries = [(query_count, "count", Org.FUSED),
                 (query_sum, "sum", Org.FUSED),
                 (query_count_siloed, "count", Org.SILOED),
                 (query_sum_siloed, "sum", Org.SILOED)]

def bench_parquet(type_range):
    print("benchmarking parquet data")
    spark = SparkSession.builder.master("local[1]").config("spark.executor.memory", "4g").getOrCreate()

    with open(results_csv, "a") as f_results:
        for n_types in type_range:
            paths = {}
            paths[Org.FUSED] = ["{}/*.parquet".format(Org.FUSED.org_dir(n_types))]
            paths[Org.SILOED] = glob.glob("{}/*.parquet".format(Org.SILOED.org_dir(n_types)))

            for query, description, organization in spark_queries:
                if organization == Org.FUSED and n_types > max_parquet_fused_types:
                    continue

                results = unix_time(query, spark=spark, paths=paths[organization])

                results_str = {}
                for k in list(results.keys()):
                    results_str[k] = "{:.2f}".format(results[k])

                fields = ["parquet", organization.value, description, n_types,
                          results_str['real'], results_str['user'],
                          results_str['sys'],
                          "{:.2f}".format(results['real'] * US_PER_S / n_types),
                          results_str['return']]
                f_results.write(",".join(str(x) for x in fields) + "\n")
                f_results.flush()

def get_file_sizes(type_range):
    print("measuring file sizes")
    with open(sizes_csv, "w") as f_sizes:
        f_sizes.write("format,organization,n_types,size\n")

        for n_types in type_range:
            for org in Org:
                for data_format in all_formats:
                    size = 0
                    paths = glob.glob("{}/*.{}".format(org.org_dir(n_types),
                                                       data_format))
                    for path in paths:
                        size += os.path.getsize(path)

                    if size > 0:
                        size_mib = size / (1024*1024.0)
                        fields = [data_format, org.value, n_types,
                                  "{:.2f}".format(size_mib)]
                        f_sizes.write(",".join(str(x) for x in fields) + "\n")

def main():
    type_range = get_type_range()
    make_dirs(type_range)

    # create all data
    create_zed_data(type_range)
    create_parquet_data(type_range)

    # measure file sizes
    get_file_sizes(type_range)

    # run benchmarks
    with open(results_csv, "w") as f_results:
        f_results.write("format,organization,query,n_types,real,user,sys,us_per_type,result\n")
    bench_zed(type_range)
    bench_parquet(type_range)

if __name__ == '__main__':
    main()
