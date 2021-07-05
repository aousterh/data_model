#!/usr/bin/env python3

# run with taskset to restrict to one core!
# taskset -c 1 ./bench_type_contexts.py

from util import unix_time_bash
import os
import random
import string

US_PER_S = 1000 * 1000
n_records = 1000 * 1000
n_data_points = 10
results_dir = "type_context_results"

# query, query description
queries = [("by typeof(.) | count()", "by typeof count"),
           ("count()", "count")]
zed_formats = ["zng", "zst"]

def bench(zed_file_info):
    # benchmark some queries over each file
    with open(results_dir + "/results.csv", "w") as f_results:
        f_results.write("format,query,n_types,real,user,sys,us_per_type\n")
        for n_types, base_name in zed_file_info:

            for zed_format in zed_formats:
                file_name = base_name + "." + zed_format

                # issue queries
                for query, description in queries:
                    results = unix_time_bash("zq -i " + zed_format +
                                             " -Z \"" + query + "\" " +
                                             file_name)
                    fields = [zed_format, description, n_types, results['real'],
                              results['user'], results['sys'],
                              "{:.2f}".format(results['real'] * US_PER_S / n_types)]
                    f_results.write(",".join(str(x) for x in fields) + "\n")

def create_data():
    type_range = [x for x in range(0, n_records + 1, int(n_records / n_data_points))]
    type_range[0] = 1 # extreme case is 1 type, not 0

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

        for n_types, base_name in zed_file_info:
            if n_types == 1:
                type_name = 0
            else:
                type_name = i % n_types

            f = zson_files[base_name]
            f.write("{{\"{:0>6}\":\"{}\"}}\n".format(type_name, letter))

    for f in zson_files.values():
        f.close()

    # create ZNG and ZST files from ZSON
    for n_types, base_name in zed_file_info:
        for zed_format in zed_formats:
            file_name = base_name + "." + zed_format
            os.system("zq -f " + zed_format + " -o " + file_name + " " + base_name + ".zson")

    return zed_file_info

def main():
    os.system("rm -fr " + results_dir)
    os.system("mkdir " + results_dir)

    zed_file_info = create_data()
    bench(zed_file_info)

if __name__ == '__main__':
    main()
