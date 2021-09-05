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
meta = None
index_rule_id = None

class Query(ABC):
    @abstractmethod
    def get_query(self, args):
        pass

    def get_range(self, args):
        # not all queries require a range
        return ""

    def get_flags(self, args, input_format):
        # not all queries require extra flags
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

    def get_flags(self, args, input_format):
        assert(len(args) == 1)

        if input_format == "lake" and meta.get("search_flag", False):
            return "-search {}:{}".format(index_rule_id, args[0])
        else:
            return ""

    def get_validation(self, results):
        return len(results.rstrip("\n").split("\n"))

    def __str__(self):
        return "search id.orig_h"

class SearchSortHeadQuery(Query):
    def get_query(self, args):
        assert(len(args) == 1)
        return 'id.orig_h=={} | sort ts | head 1000'.format(*args)

    def get_flags(self, args, input_format):
        assert(len(args) == 1)

        if input_format == "lake" and meta.get("search_flag", False):
            return "-search {}:{}".format(index_rule_id, args[0])
        else:
            return ""
    
    def get_validation(self, results):
        l = results.rstrip("\n").split("\n")[-1]
        return re.search(r"orig_p:(\d*)\(port", l).groups()[0]

    def __str__(self):
        return "search sort head id.orig_h"

class AnalyticsRangeTsSumQuery(Query):
    def get_query(self, args):
        assert(len(args) == 2)
        return 'ts >= {} ts < {} | sum(orig_bytes)'.format(*args)

    def get_range(self, args):
        assert(len(args) == 2)
        return "over {} to {}".format(*args)

    def get_validation(self, results):
        return re.search(r"sum:(\d*)\(uint64\)", results).groups()[0]

    def __str__(self):
        return "analytics range ts sum orig_bytes"

class AnalyticsAvgQuery(Query):
    def get_query(self, args):
        assert(len(args) == 1)
        return 'avg({})'.format(*args)

    def get_flags(self, args, input_format):
        assert(len(args) == 1)

        if input_format == "zst" and meta.get("zst_cutter_flag", False):
            return "-k {}".format(*args)
        else:
            return ""

    def get_validation(self, results):
        return re.search(r"avg:(\d+(\.\d*)?|\.\d+)", results).groups()[0]

    def __str__(self):
        return "analytics avg field"

# mapping from string ID of each query to its Query class
QUERIES = {str(q): q for q in [SearchQuery(), SearchSortHeadQuery(),
                               AnalyticsRangeTsSumQuery(), AnalyticsAvgQuery()]}

def data_path(fmt):
    if meta.get("input_one_file", True):
        return "{}/all.{}".format(DATA, fmt)
    else:
        return "{}/{}/*".format(DATA, fmt)

zq_cmd = "zq -validate=false -i {} {} \"{}\" {}"
zed_lake_cmd = "zed lake query {} \"from p1 {} | {}\""

def create_lake():
    log_dir = os.path.join(os.getcwd(), "logs")
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.mkdir(log_dir)

    os.environ["ZED_LAKE_ROOT"] = log_dir
    os.system("zed lake init")

def setup_lake():
    create_lake()

    os.system("zed lake create -p p1")
    os.system("zed lake load -p p1 {}".format(data_path("zng")))

def setup_lake_hack():
    global index_rule_id
    create_lake()

    os.system("zed lake create -p p1 -S 20MB")
    os.system("zed lake load -p p1 {}".format(data_path("zng")))

    # create index and get ID
    results = os.popen("zed lake index create TEST field id.orig_h").read()
    index_rule_id = None
    for l in results.split("\n"):
        m = re.search(r"\s*rule (.*) field ", l)
        if m is not None:
            index_rule_id = m.groups()[0]

    results = os.popen("zed lake log -p p1").read()

    # parse out object IDs
    ids = []
    for l in results.split("\n"):
        m = re.search(r"\s*(.*)\s+(\d+) records in", l)
        if m is not None:
            ids.append(m.groups()[0])

    # apply index to objects
    for i in ids:
        os.system("zed lake index apply -p p1 TEST {}".format(i))

def run_benchmark(query_description, f_input, f_output=sys.stdout,
                  input_fmt="zng", output_fmt="zng"):
    flush_buffer_cache()

    start_time = time.time()
    index = 0

    # assume only a single type of query per trace
    query = QUERIES[query_description]
    for line in f_input:
        args = json.loads(line)["arguments"]

        zq_query = query.get_query(args)

        flags = []
        # output format flag
        if output_fmt == "zson":
            flags.append("-z")
        else:
            flags.append("-f {}".format(output_fmt))

        # flags for various hacks
        flags.append(query.get_flags(args, input_fmt))

        flags_str = " ".join(flags)

        if input_fmt == "lake":
            query_range = query.get_range(args)
            cmd = zed_lake_cmd.format(flags_str, query_range, zq_query)
        else:
            cmd = zq_cmd.format(input_fmt, flags_str, zq_query,
                                data_path(input_fmt))
        query_time = time.time()

        results = unix_time_bash(cmd, stdout=subprocess.PIPE)

        validation = query.get_validation(results["return"])
        fields = [index, "zed", input_fmt, output_fmt, query_description,
                  round(query_time - start_time, 3), results["real"],
                  results["user"], results["sys"], args[0], validation,
                  meta.get("instance", "unknown")]
        f_output.write(",".join([str(x) for x in fields]) + "\n")

        index += 1

def main():
    global config, meta

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

    setup_lake_hack()

    with open(RESULTS_CSV, 'w') as f_output:
        f_output.write("index,system,in_format,out_format,query,start_time,real,user,sys,argument_0,validation,instance\n")

        for workload, queries in benchmark.items():
            w_config = workload_config(workload)
            for q in queries:
                q_config = w_config.get("query").get(q)
                t_file = trace_file(q_config.get("trace_file"))

                for (input_fmt, output_fmt) in formats:
                    with open(t_file, 'r') as f_input:
                        run_benchmark(q_config.get("desc"), f_input, f_output,
                                      input_fmt, output_fmt)

if __name__ == '__main__':
    main()
