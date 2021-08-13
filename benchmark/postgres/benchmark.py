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

    def connect(self):
        self.conn = util.db_conn()
        return self

    def run(self):
        cursor = self.conn.cursor()

        # get all tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE (table_schema = 'public')
            ORDER BY table_name
        """)
        tables = [r[0] for r in cursor.fetchall()]

        if self._meta.get("warmup", True):
            raise NotImplemented

        # run benchmarks
        for workload in self._benchmark:
            wc = util.workload_config(workload)

            for name, param in wc["query"].items():
                start = time.time()
                results = list()
                query_funcs = list()

                # create / drop index
                for t in tables:
                    _update_index(t, param["field"],
                                  drop=not param.get("index", False))
                if wc["kind"] == "search":
                    def make_exec(_v):
                        def _exec():
                            if len(tables) > 1:
                                pool = Pool(self._meta.get("num_thread", 1))
                                _output = pool.starmap(_search, [(t, param["field"], _v)
                                                                 for t in tables])
                            else:
                                _output = _search(tables[0], param["field"], _v)
                            return _output

                        return _exec

                    # iterate the values
                    values = list()
                    tf = param.get("trace_file", None)
                    if tf:
                        values = [row["arguments"][0] for row in util.read_trace(tf)]
                        values = values[:param.get("batch_size", len(values))]
                    for v in values:
                        query_funcs.append((make_exec(v), v))

                elif wc["kind"] == "analytics":
                    def make_exec(_s, _e):
                        def _exec():
                            if len(tables) > 1:
                                pool = Pool(self._meta.get("num_thread", 1))
                                _output = pool.starmap(_range_sum, [(t, param["field"],
                                                                    param["target"], _s, _e)
                                                                    for t in tables])
                            else:
                                _output = _range_sum(tables[0], param["field"],
                                                     param["target"], _s, _e)
                            return _output

                        return _exec

                    # iterate the values
                    values = list()
                    tf = param.get("trace_file", None)
                    if tf:
                        values = [row["arguments"] for row in util.read_trace(tf)]
                        values = values[:param.get("batch_size", len(values))]
                    for _start, _end in values:
                        query_funcs.append((make_exec(_start, _end), f"{_start} {_end}"))
                else:
                    raise NotImplemented

                # execute
                for i, (f, arg) in enumerate(query_funcs):
                    print(f"progress: running with {i + 1}/{len(query_funcs)}")
                    r = util.benchmark(f, num_iter=self._meta.get("num_run", 1))

                    # dump to log
                    if wc["kind"] == "search":
                        val = sum(len(t) for t in r["return"] if t is not None)
                    elif wc["kind"] == "analytics":
                        val = sum(t[0][0] for t in r["return"] if t is not None)

                    results.append(OrderedDict({
                        "index": i,
                        "system": "postgres",
                        "in_format": "index" if param.get("index", False) else "table",
                        "out_format": "table",
                        "query": param["desc"],
                        "start_time": round(time.time() - start, 3),
                        "real": r["real"],
                        "user": r["user"],
                        "sys": r["sys"],
                        "argument_0": arg,
                        "validation": val,
                    }))
            util.write_csv(results, f"postgres-{wc['kind']}-{name}{'-index' if param.get('index', False) else ''}.csv")


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


def _range_sum(_t, _field, _target, _start, _end):
    _cursor = util.db_conn().cursor()
    s = f"""
        SELECT SUM ("{_target}") AS sum
        FROM {_t}
        WHERE "{_field}" < '{_end}' AND "{_field}" > '{_start}';
    """
    r = None
    try:
        _cursor.execute(s)
        r = _cursor.fetchall()
        print(_t, s, "success")
    except Exception as e:
        print(_t, e)
    return r


def _search(_t, _k, _v):
    _cursor = util.db_conn().cursor()
    s = f"""
        SELECT * FROM {_t}
        WHERE ("{_k}" = '{_v}');
    """
    r = None
    try:
        _cursor.execute(s)
        r = _cursor.fetchall()
        print(_t, s, "success")
    except Exception as e:
        print(_t, e)
    return r


def main():
    Benchmark().connect().run()


if __name__ == '__main__':
    main()
