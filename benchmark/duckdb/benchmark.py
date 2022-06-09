#!/usr/bin/env python3
from abc import ABC, abstractmethod
from datetime import datetime
import glob
import json
import sys
sys.path.insert(1, "..")
from util import *

# assume volume is mounted at /zq-sample-data
BASE_DIR = "/zq-sample-data"
BASE_DIR = "/local/zeek-data-all/subset_80_million"
DATA = BASE_DIR + "/z"
#BASE_DIR = "/home/admin"
#DATA = "/home/admin/zed-sample-data"

DATA_FILE = "data.duckdb"
RESULTS_CSV = "duckdb.csv"
meta = None
# mapping from table name to list of columns
schemas = {}

class Query(ABC):
    @abstractmethod
    def get_query(self, args):
        pass

    @abstractmethod
    def get_validation(self, results):
        pass

    @abstractmethod
    def __str__(self):
        pass

class AnalyticsAvgQuery(Query):
    def get_query(self, args):
        assert(len(args) == 1)

        # TODO: could potentially accelerate if this field is only present in one table

        column_name = args[0]
        field_name = column_name.split(".")[-1]
        query_str = "SELECT AVG({}) FROM (".format(field_name)

        first_table = True
        for table in schemas.keys():
            if field_name not in schemas[table]:
                continue

            if first_table:
                query_str += "SELECT {} FROM {}".format(column_name, table)
                first_table = False
            else:
                query_str += " UNION ALL SELECT {} FROM {}".format(column_name, table)
        query_str += ")"

        return query_str

    def get_validation(self, results):
        return results.split("\n")[3].split(" ")[1]

    def __str__(self):
        return "analytics avg field"

# mapping from string ID of each query to its Query class
QUERIES = {str(q): q for q in [AnalyticsAvgQuery()]}

def duckdb_cmd(cmd, restrict_cores=False):
    base_cmd = "./duckdb {} -c \"{}\"".format(DATA_FILE, cmd)
    if restrict_cores and meta.get("cores_mask"):
        return "taskset -c {} ".format(meta.get("cores_mask")) + base_cmd
    else:
        return base_cmd
    
def exec_duckdb(cmd, return_output=False, restrict_cores=False):
    complete_cmd = duckdb_cmd(cmd, restrict_cores)
    print("about to exec: {}".format(complete_cmd))

    if not return_output:
        os.system(complete_cmd)
    else:
        result = subprocess.run(complete_cmd, shell=True, text=True,
                                stdout=subprocess.PIPE)
        return result.stdout

def setup():
    # remove old duckdb database
    if os.path.exists(DATA_FILE):
        os.remove(DATA_FILE)

    exec_duckdb(".tables")

    # load all of the tables from parquet to preserve the nested structure
    files = glob.glob(os.path.join(DATA, "parquet/*.parquet"))
    for f in files:
        table_name = os.path.basename(f).split(".")[0]
        exec_duckdb("CREATE TABLE {} AS SELECT * FROM '{}';"
                    .format(table_name, f))
        schemas[table_name] = []

    exec_duckdb(".tables")

    # retrieve and parse schemas
    for table in schemas.keys():
        query = """SELECT column_name, data_type FROM
        INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'""".format(table)
        schema_str = exec_duckdb(query, return_output=True)

        # parse schema
        for l in schema_str.split("\n"):
            if "column_name" in l:
                continue
            if "───" in l:
                continue
            if l == "":
                continue

            field_name = l.split(" ")[1]

            schemas[table].append(field_name)

    # check how much data was loaded
    total_rows = 0
    for table in schemas.keys():
        query = "SELECT COUNT(*) FROM {}".format(table)
        count_str = exec_duckdb(query, return_output=True)

        # parse output
        count = int(count_str.split("\n")[3].split(" ")[1])
        print("loaded {} rows into table {}".format(count, table))
        total_rows += count

    print("loaded {} rows".format(total_rows))

def run_benchmark(query_description, f_input, f_output=sys.stdout):
    flush_buffer_cache()

    start_time = time.time()
    index = 0

    # assume only a single type of query per trace
    query = QUERIES[query_description]
    for line in f_input:
        args = json.loads(line)["arguments"]

        sql_query = query.get_query(args)

        query_time = time.time()

        cmd = duckdb_cmd(sql_query, restrict_cores=True)
        print("issuing query: {}".format(cmd))
        results = unix_time_bash(cmd, stdout=subprocess.PIPE)

        validation = query.get_validation(results["return"])
        fields = [index, "duckdb", "database", "stdout", query_description,
                  round(query_time - start_time, 3), results["real"],
                  results["user"], results["sys"], args[0], validation,
                  meta.get("instance", "unknown")]
        f_output.write(",".join([str(x) for x in fields]) + "\n")

        index += 1
    
def main():
    global config, meta

    # read in config
    config_file = os.environ.get('CONFIG', 'default.yaml')
    with open(config_file) as f:
        config = yaml.load(f, Loader=yaml.Loader)
        meta = config.get("meta", {})
        benchmark = config.get("benchmark", {})

    before = datetime.now()
    print("starting setup at: {}".format(before))
    setup()
    after = datetime.now()
    print("finished setup at: {}, took: {}".format(after, after - before))

    before = datetime.now()
    with open(RESULTS_CSV, 'w') as f_output:
        f_output.write("index,system,in_format,out_format,query,start_time,real,user,sys,argument_0,validation,instance\n")

        for workload, queries in benchmark.items():
            w_config = workload_config(workload)
            for q in queries:
                q_config = w_config.get("query").get(q)
                t_file = trace_file(q_config.get("trace_file"))

                with open(t_file, 'r') as f_input:
                    run_benchmark(q_config.get("desc"), f_input, f_output)
    after = datetime.now()
    print("finished querying at: {}, took: {}".format(after, after - before))

if __name__ == '__main__':
    main()
