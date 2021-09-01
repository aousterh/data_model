#!/usr/bin/env python3
from abc import ABC, abstractmethod
import json
import os
import re
import shutil
import sys
import sys
import yaml
sys.path.insert(1, "..")
from util import *

# assume volume is mounted at /zq-sample-data
BASE_DIR = "/zq-sample-data"
DATA = BASE_DIR + "/z"
RESULTS_CSV = "end_to_end_zed.csv"
config = None

class Query(ABC):
    @abstractmethod
    def get_query(self, args):
        pass

    def get_range(self, args):
        # not all queries require a range
        return ""

    @abstractmethod
    def get_validation(self, results):
        pass

    @abstractmethod
    def __str__(self):
        pass

class SearchQuery(Query):
    def get_query(self, args):
        assert(len(args) == 1)
        return 'id.orig_h=={}'.format(*args)

    def get_validation(self, results):
        return len(results.rstrip("\n").split("\n"))

    def __str__(self):
        return "search id.orig_h"

class AnalyticsQuery(Query):
    def get_query(self, args):
        assert(len(args) == 2)
        return 'ts >= {} ts < {} | sum(orig_bytes)'.format(*args)

    def get_range(self, args):
        assert(len(args) == 2)
        return "over {} to {}".format(*args)
        return ""

    def get_validation(self, results):
        return re.search(r"sum:(\d*)\(uint64\)", results).groups()[0]

    def __str__(self):
        return "analytics sum orig_bytes"

# mapping from string ID of each query to its Query class
QUERIES = {str(q): q for q in [SearchQuery(), AnalyticsQuery()]}

def data_path(fmt):
    if config.get("meta", {}).get("input_one_file", True):
        return "{}/all.{}".format(DATA, fmt)
    else:
        return "{}/{}/*".format(DATA, fmt)

zq_cmd = "zq -validate=false -i {} {} \"{}\" {}"
zed_lake_cmd = "zed lake query {} \"from logs {} | {}\""

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
    flush_buffer_cache()

    start_time = time.time()
    index = 0

    for line in f_input:
        query_description = json.loads(line)
        query = QUERIES[query_description["query"]]
        args = query_description["arguments"]

        zq_query = query.get_query(args)

        if output_fmt == "zson":
            output_fmt_string = "-z"
        else:
            output_fmt_string = "-f {}".format(output_fmt)

        if input_fmt == "lake":
            query_range = query.get_range(args)
            cmd = zed_lake_cmd.format(output_fmt_string, query_range, zq_query)
        else:
            cmd = zq_cmd.format(input_fmt, output_fmt_string, zq_query,
                                data_path(input_fmt))
        query_time = time.time()
        results = unix_time_bash(cmd, stdout=subprocess.PIPE)

        validation = query.get_validation(results["return"])
        fields = [index, "zed", input_fmt, output_fmt, query,
                  round(query_time - start_time, 3), results["real"],
                  results["user"], results["sys"], args[0], validation,
                  config.get("meta", {}).get("instance", "unknown")]
        f_output.write(",".join([str(x) for x in fields]) + "\n")

        index += 1

def main():
    global config

    formats = [("zng", "zson"),
               ("zst", "zson"),
               ("lake", "zson")]
#               ("zson", "zson")]

    # read in config
    config_file = os.environ.get('CONFIG', 'default.yaml')
    with open(config_file) as f:
        config = yaml.load(f, Loader=yaml.Loader)
        meta = config.get("meta", {})
        benchmark = config.get("benchmark", {})

    create_archive()

    with open(RESULTS_CSV, 'w') as f_output:
        f_output.write("index,system,in_format,out_format,query,start_time,real,user,sys,argument_0,validation,instance\n")

        for workload in benchmark:
            w_config = workload_config(workload)
            t_file = trace_file(list(w_config.get("query").values())[0].get("trace_file"))

            for (input_fmt, output_fmt) in formats:
                with open(t_file, 'r') as f_input:
                    run_benchmark(f_input, f_output, input_fmt, output_fmt)
    
if __name__ == '__main__':
    main()
