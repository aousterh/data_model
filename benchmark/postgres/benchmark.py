#!/usr/bin/env python3

import os
import sys

import numpy as np
import yaml
import time
from multiprocessing import Pool
from collections import OrderedDict, defaultdict

import util


class Benchmark:
    def __init__(self):
        _c_file = os.environ.get('CONFIG', 'default') + ".yaml"
        with open(_c_file) as f:
            self._config = yaml.load(f, Loader=yaml.Loader)
            self._meta = self._config.get("meta", {})
            self._benchmark = self._config.get("benchmark", {})

        self.conn = None
        self.tables = None
        self.table_columns = defaultdict(dict)
        self.uber_schema: dict = OrderedDict()

    def connect(self):
        self.conn = util.db_conn()

        # fetch all tables
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE (table_schema = 'public')
            ORDER BY table_name
        """)
        self.tables = [r[0] for r in cursor.fetchall()]

        for t in self.tables:
            cursor.execute(f"""
                SELECT column_name, data_type FROM 
                INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{t}';
            """)
            columns = {r[0]: r[1] for r in cursor.fetchall() if r[0]}
            self.table_columns[t] = columns
            self.uber_schema = {**self.uber_schema, **columns}
        return self

    def run(self):
        if self._meta.get("warmup", True):
            raise NotImplemented

        # read workloads
        for workload, queries in self._benchmark.items():

            for name in queries:
                # query configs
                qc = util.workload_config(workload, query=name)

                # query executor
                use_union = self._meta.get("use_union", [])
                use_uber_schema = self._meta.get("use_uber_schema", [])

                def make_exec(_param):
                    def _exec():
                        if len(self.tables) == 1 or name in use_union:
                            return _query(self.tables, _param)

                        with Pool(self._meta.get("num_thread", 1)) as pool:
                            return pool.starmap(_query, [([_t], _param)
                                                         for _t in self.tables])

                    return _exec

                # unpack args and create executors
                executors = list()
                tf = qc.get("trace_file", None)
                if tf is None:
                    args = qc.get("args")
                else:
                    args = [row["arguments"] for row in util.read_trace(tf)]

                for arg in args:
                    params = {**qc, **{"name": name, "arg": arg,
                                       "schemas": self.table_columns,
                                       "use_union": name in use_union,
                                       "uber_schema": self.uber_schema if name in use_uber_schema else None,
                                       }}
                    executors.append((make_exec(params), params))

                # create / drop index
                _f = qc.get("field", "")
                for _t in self.tables:
                    if _f not in self.table_columns:
                        continue
                    if "index" in qc:
                        _drop = not qc.get("index", False)
                        _update_index(_t, _f, drop=_drop)

                # run executors
                start, results = time.time(), list()
                for i, (f, params) in enumerate(executors):
                    print(f"progress: running with {i + 1}/{len(executors)}")

                    r = util.benchmark(f, num_iter=self._meta.get("num_run", 1))

                    # get validate and dump to log
                    results.append(OrderedDict({
                        "index": i,
                        "system": "postgres",
                        "in_format": "index" if qc.get("index", False) else "table",
                        "out_format": "table",
                        "query": qc["desc"],
                        "start_time": round(time.time() - start, 3),
                        "real": r["real"],
                        "user": r["user"],
                        "sys": r["sys"],
                        "argument_0": params["arg"],
                        "validation": _get_validate(name, r),
                        "instance": self._meta.get("instance", "unknown"),
                    }))

                f_name = f"postgres-{qc.get('desc', '').replace(' ', '_').replace('.', '_')}.csv"
                util.write_csv(results, f_name)


def _update_index(_t, _k, drop=False):
    _conn = util.db_conn()
    _cursor = _conn.cursor()
    idx_name = f"{_t}_{_k.replace('.', '_')}"

    s = f"""
        CREATE INDEX "{idx_name}" ON {_t} ("{_k}");
    """ if not drop else f"""
        DROP INDEX "{idx_name}";
    """
    try:
        _cursor.execute(s)
        _conn.commit()
        _cursor.close()
        print(_t, s, "success")
    except Exception as e:
        print(_t, e)


def _query(tables, params):
    r, s, = None, ""

    name = params.get("name", "")
    for i, _t in enumerate(tables):

        ### Agg
        if name in {"avg"}:
            # field to avg
            _f = params['arg'][0]

            # take agg on table union
            if params.get("use_union", False):
                # make the query string..
                # on last table:
                if i == len(tables) - 1 and s != "":
                    s += ") as sub_query;"

                # skip the table if it doesn't have the field
                if _f not in params["schemas"][_t]:
                    continue

                # on first table that contains the field:
                if s == "":
                    s = f"""
                        SELECT AVG ("{params['arg'][0]}") 
                        FROM (
                        SELECT "{params['arg'][0]}" FROM {_t}
                    """
                # on additional table:
                else:
                    s += f"""
                        UNION ALL
                        SELECT "{params['arg'][0]}" FROM {_t}
                    """
            # ..or with map-reduce style
            else:
                raise NotImplemented

        ### Search
        elif name in {"search", "search_no_index",
                      "search_sort_head", "search_sort_head_no_index"}:

            # on last table
            if i == len(tables) - 1 and s != "":
                if name in {"search_sort_head",
                            "search_sort_head_no_index"}:
                    s += f"""
                        ORDER by "{params['sort_field']}"
                        LIMIT 1000"""
                s += ";"

            # on missing field
            if params["field"] not in params["schemas"][_t]:
                continue

            # on additional tables
            if params.get("use_union", False) and s != "":
                s += f"""
                   UNION ALL """

            columns = ""
            uber_schema = params.get("uber_schema")
            if uber_schema is not None:
                schema = set(params["schemas"][_t])
                for _name, _typ in uber_schema.items():
                    if _name not in schema:
                        columns += f""", NULL::{_typ} AS "{_name}" """
                    else:
                        columns += f""", CAST("{_name}" AS {_typ}) "{_name}" """
            else:
                columns = "*"
            columns = columns.lstrip(", ")
            s += f"""
                SELECT {columns} FROM {_t}
                WHERE ("{params['field']}" = '{params['arg'][0]}')"""

        ### Range agg
        elif name in {"range_sum", "range_sum_no_index"}:
            s = f"""
                SELECT SUM ("{params['target']}") AS sum
                FROM {_t}
                WHERE "{params['field']}" < '{params['arg'][1]}' 
                    AND "{params['field']}" > '{params['arg'][0]}';
            """

        else:
            raise NotImplemented

    if s == "":
        return r

    # execute
    _cursor = util.db_conn().cursor()
    try:
        _cursor.execute(s)
        r = _cursor.fetchall()
        # print(_ts, s, "success")
    except Exception as e:
        print("error:", e)
        return None
    return r


def _get_validate(name, r):
    if name in {"search", "search_no_index"}:
        return sum([len(t) for t in r["return"] if t is not None])
    elif name in {"search_sort_head", "search_sort_head_no_index"}:
        # XXX orig_p of the last column; should be oblivious to the dataset
        return r["return"][-1][11]
    elif name in {"range_sum", "range_sum_no_index"}:
        return sum(t[0][0] for t in r["return"] if t is not None)
    elif name in {"avg"}:
        return np.average([float(t[0]) for t in r["return"] if t is not None])
    return ""


def main():
    Benchmark().connect().run()


if __name__ == '__main__':
    main()
