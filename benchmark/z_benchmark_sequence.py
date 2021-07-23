#!/usr/bin/env python3
import json
import os
import shutil
import sys
from util import *

# assume volume is mounted at /zq-sample-data
BASE_DIR = "/zq-sample-data"
DATA = BASE_DIR + "/z"
WORKLOAD = "workload/workload.ndjson"
RESULTS_CSV = "results.csv"

def data_path(fmt):
    return DATA + "/" + fmt + "/*"

zq_cmd = "zq -i {} -f {} \"{}\" {}"
zed_lake_cmd = "zed lake query -f {} \"from logs | {}\""

queries = {
    'search id.orig_h': 'id.orig_h=={}',
    'search id.orig_h + count id.resp_h': 'id.orig_h=={} | count() by id.resp_h',
}

def create_archive():
    log_dir = os.path.join(os.getcwd(), "logs")
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.mkdir(log_dir)

    os.environ["ZED_LAKE_ROOT"] = log_dir
    os.system("zed lake init logs")
    os.system("zed lake create -p logs")
    os.system("zed lake load -p logs {}".format(data_path("zng")))

def run_benchmark(f_input, f_output=sys.stdout, input_fmt="zng",
                  output_fmt="zng"):
    start_time = time.time()
    index = 0

    for line in f_input:
        query_description = json.loads(line)
        query = query_description["query"]
        arg0 = query_description["arguments"][0]
        zq_query = queries[query].format(arg0)

        if input_fmt == "archive":
            cmd = zed_lake_cmd.format(output_fmt, zq_query)
        else:
            cmd = zq_cmd.format(input_fmt, output_fmt, zq_query,
                                data_path(input_fmt))
        query_time = time.time()
        results = unix_time_bash(cmd)
        fields = [index, "zed", input_fmt, output_fmt, query,
                  round(query_time - start_time, 3), results["real"],
                  results["user"], results["sys"], arg0]
        f_output.write(",".join([str(x) for x in fields]) + "\n")

        index += 1

def main():
    create_archive()

    formats = [("zng", "zng"),
#               ("zst", "zst"),
               ("zst", "zng"),
               ("archive", "zng")]
#               ("zson", "zson")]

    with open(RESULTS_CSV, 'w') as f_output:
        f_output.write("index,system,in_format,out_format,query,start_time,real,user,sys,argument_0\n")

        for (input_fmt, output_fmt) in formats:
            flush_buffer_cache()

            with open(WORKLOAD, 'r') as f_input:
                run_benchmark(f_input, f_output, input_fmt, output_fmt)
    
if __name__ == '__main__':
    main()
