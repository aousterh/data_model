#!/usr/bin/env python3

import os
import yaml
import time
from multiprocessing import Pool
from collections import OrderedDict

import util
import psycopg2


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

                if wc["kind"] == "search":
                    # create / drop index
                    for t in tables:
                        _update_index(t, param["field"],
                                      drop=not param.get("index", False))

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
                    raise NotImplemented

                else:
                    raise NotImplemented

                for i, (f, arg) in enumerate(query_funcs):
                    print(f"progress: running with {i + 1}/{len(query_funcs)}")
                    r = util.benchmark(f, num_iter=self._meta.get("num_run", 1))

                    # dump to log
                    results.append(OrderedDict({
                        "index": i,
                        "system": "postgres",
                        "in_format": "index" if param.get("index", False) else "bson",
                        "out_format": "table",
                        "query": param["desc"],
                        "start_time": round(time.time() - start, 3),
                        "real": r["real"],
                        "user": r["user"],
                        "sys": r["sys"],
                        "argument_0": arg,
                        "validation": sum(len(t) for t in r["return"] if t is not None)
                    }))
            util.write_csv(results, f"postgres-{wc['kind']}-{name}.csv")


def _update_index(_t, _k, drop=False):
    _cursor = util.db_conn().cursor()
    s = f"""
        CREATE INDEX "{_k}" ON {_t} ("{_k}" ASC);
    """ if not drop else """
        DROP INDEX "{_k}";
    """
    try:
        _cursor.execute(s)
    except Exception as e:
        pass


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
    except psycopg2.errors.UndefinedColumn:
        pass
    return r


def main():
    Benchmark().connect().run()


if __name__ == '__main__':
    main()
