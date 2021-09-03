#!/usr/bin/env python3

import os
import sys
import yaml
import time
from multiprocessing import Pool
from collections import OrderedDict

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

        return self

    def run(self):
        if self._meta.get("warmup", True):
            raise NotImplemented

        # read workloads
        for workload, queries in self._benchmark.items():

            for name in queries:
                # query configs
                qc = util.workload_config(workload, query=name)

                # make query executor
                def make_exec(_param):
                    def _exec():
                        if len(self.tables) == 1 or self._meta.get("use_union", False):
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
                    params = {**qc, **{"name": name, "arg": arg}}
                    executors.append((make_exec(params), params))

                # create / drop index
                for _t in self.tables:
                    if "index" in qc:
                        _drop = not qc.get("index", False)
                        _update_index(_t, qc.get("field"), drop=_drop)

                # run
                start, results = time.time(), list()
                for i, (f, arg) in enumerate(executors):
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
                        "argument_0": arg,
                        "validation": _get_validate(name, r),
                        "instance": self._meta.get("instance", "unknown"),
                    }))

                f_name = f"postgres-{qc.get('desc', '').replace(' ', '_')}.csv"
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


def _query(_ts, params):
    r, s, = None, None

    name = params.get("name", "")
    for _t in _ts:
        if name == "avg":
            s = f"""
                SELECT AVG ("{params['arg'][0]}") AS avg
                FROM {_t}
                WHERE false;
            """
        elif name == "search":
            s = f"""
                SELECT * FROM {_t}
                WHERE ("{params['field']}" = '{params['arg'][0]}');
            """
        elif name == "search_sort_head":
            # TBD
            s = f"""
                    SELECT * FROM {_t}
                    WHERE ("{params['field']}" = '{params['arg'][0]}');
                """
        elif name in {"range_sum", "range_sum_no_index"}:
            s = f"""
                SELECT SUM ("{params['target']}") AS sum
                FROM {_t}
                WHERE "{params['field']}" < '{params['arg'][1]}' 
                    AND "{params['field']}" > '{params['arg'][0]}';
            """
        else:
            s = ""

    # execute
    _cursor = util.db_conn().cursor()
    try:
        _cursor.execute(s)
        r = _cursor.fetchall()
        # print(_ts, s, "success")
    except Exception as e:
        print(_ts, e)
    return r


def _get_validate(name, r):
    if name in {"search"}:
        return sum(len(t) for t in r["return"] if t is not None)
    elif name in {"range_sum", "range_sum_no_index"}:
        return sum(t[0][0] for t in r["return"] if t is not None)
    elif name in {"avg"}:
        return list(t[0][0] for t in r["return"] if t is not None)
    return ""


def main():
    Benchmark().connect().run()


if __name__ == '__main__':
    main()
