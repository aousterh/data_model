#!/usr/bin/env python3
import os
import shutil
from util import benchmark_bash

# assume volume is mounted at /zq-sample-data
BASE_DIR = "/zq-sample-data"
DATA = BASE_DIR + "/z"

def data_path(fmt):
    return DATA + "/" + fmt + "/*"

zq_uncomp_query = "zq -i {} -f zng -znglz4blocksize 0 '{}' {}"
zq_query = "zq -i {} -f {} \"{}\" {}"
zar_query = "zar zq -f {} \"{}\""

# specific zq filters
analytics = 'count() by id.orig_h'
search = 'id.orig_h=10.128.0.19'
search_sort = 'id.orig_h=10.128.0.19 | sort ts'
search_sort_top_5 = 'id.orig_h=10.128.0.19 | sort ts | head 5'
discovery = 'count() by typeof(.)'

all_queries = [analytics, search, search_sort, search_sort_top_5, discovery]

inputs = ["zng", "zng-uncompressed", "zst"]
outputs = ["zng", "zng-uncompressed", "zst"]

def create_archive():
    log_dir = os.path.join(os.getcwd(), "logs")
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.mkdir(log_dir)

    os.environ["ZAR_ROOT"] = log_dir
    os.system("zq {} | zar import -s 25MB -".format(data_path("zng")))

def main():

    create_archive()

    print("query,input,ouput,real,user,sys")
    print("-------------------------------")

    for q in all_queries:
        for output in outputs:
            # issue query using an archive
            if output == "zng-uncompressed":
                output_fmt = "zng"
            else:
                output_fmt = output

            cmd = zar_query.format(output_fmt, q)
            results = benchmark_bash(cmd)
            fields = [q, "archive", output, results["real"], results["user"],
                      results["sys"]]
            print(",".join([str(x) for x in fields]))

            # issue query over other input formats
            for input in inputs:
                if input == "zng-uncompressed":
                    input_fmt = "zng"
                else:
                    input_fmt = input

                if output == "zng-uncompressed":
                    cmd = zq_uncomp_query.format(input_fmt, q, data_path(input))
                else:
                    cmd = zq_query.format(input_fmt, output_fmt, q,
                                          data_path(input))

                results = benchmark_bash(cmd)
                fields = [q, input, output, results["real"], results["user"],
                          results["sys"]]
                print(",".join([str(x) for x in fields]))

if __name__ == '__main__':
    main()
