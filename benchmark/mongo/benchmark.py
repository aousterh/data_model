#!/usr/bin/env python3

import os
import time
import yaml
from multiprocessing import Pool
from collections import OrderedDict

from pymongo import MongoClient

import util


class Benchmark:
    def __init__(self):
        _c_file = os.environ.get('CONFIG', 'default') + ".yaml"
        with open(_c_file) as f:
            self._config = yaml.load(f, Loader=yaml.Loader)
            self._meta = self._config.get("meta", {})
            self._benchmark = self._config.get("benchmark", {})

        self._db: str = os.environ['DB']
        self._cli, self._cols = None, None

    def connect(self):
        self._cli = MongoClient('localhost', 27017, maxPoolSize=10000)
        self._cols = self._cli[self._db].list_collection_names()

        _col = os.environ.get("COL", None)
        if _col:
            assert _col in self._cols
            self._cols = [_col]
        return self

    def run(self):
        if self._meta.get("warmup", True):
            raise NotImplemented

        for workload, queries in self._benchmark.items():
            for name in queries:
                qc = util.workload_config(workload, query=name)

                start = time.time()
                results = list()
                query_funcs = list()

                if "index" in qc:
                    _drop = not qc.get("index", False)
                    _update_index(self._db, self._cols,
                                  qc["field"],
                                  drop=_drop)

                if name in {"search", "search_sort_head"}:
                    sort_head = True if name == "search_sort_head" else False
                    def make_exec(_v):
                        def _exec():
                            if len(self._cols) > 1:
                                with Pool(self._meta.get("num_thread", 1)) as pool:
                                    _output = pool.starmap(_search, [(self._db, c, qc["field"], _v, sort_head)
                                                                     for c in self._cols])
                            else:
                                _output = _search(self._db, self._cols[0], qc["field"], _v, sort_head)
                            return _output

                        return _exec

                    values = list()
                    tf = qc.get("trace_file", None)
                    if tf:
                        values = [row["arguments"][0] for row in util.read_trace(tf)]
                        values = values[:qc.get("batch_size", len(values))]
                    for v in values:
                        query_funcs.append((make_exec(v), v))

                elif name == "avg":
                    # TBD
                    def make_exec(_v):
                        def _exec():
                            if len(self._cols) > 1:
                                with Pool(self._meta.get("num_thread", 1)) as pool:
                                    _output = pool.starmap(_avg, [(self._db, c, _v)
                                                                  for c in self._cols])
                            else:
                                _output = _avg(self._db, self._cols[0], _v)
                            return _output

                        return _exec

                    values = list()
                    tf = qc.get("trace_file", None)
                    if tf:
                        values = [row["arguments"][0] for row in util.read_trace(tf)]
                        values = values[:qc.get("batch_size", len(values))]
                    for v in values:
                        query_funcs.append((make_exec(v), v))
                elif name == "search_sort_head":
                    raise NotImplemented

                for i, (f, arg) in enumerate(query_funcs):
                    print(f"progress: running with {i + 1}/{len(query_funcs)}")
                    r = util.benchmark(f, num_iter=self._meta.get("num_run", 1))

                    # dump to log
                    results.append(OrderedDict({
                        "index": i,
                        "system": "mongo",
                        "in_format": "index" if qc.get("index", False) else "bson",
                        "out_format": "json",
                        "query": qc["desc"],
                        "start_time": round(time.time() - start, 3),
                        "real": r["real"],
                        "user": r["user"],
                        "sys": r["sys"],
                        "argument_0": arg,
                        "validation": {
                            "search": lambda x: len(x),
                            "search_sort_head": lambda x: x[-1]["id"]["orig_p"],
                        }.get(name, lambda x: x)(r["return"]),
                        "instance": self._meta.get("instance", "unknown"),
                    }))
                util.write_csv(results, f"mongo-{name}{'-index' if qc.get('index', False) else ''}.csv")


def _avg(db, col, field):
    _c = MongoClient('localhost', 27017, maxPoolSize=10000)

    r = _c[db][col].aggregate(
        [{"$group": {"_id": None, "avg": {"$avg": f"${field}"}}}]
    )
    return list(r)[0]["avg"]


def _range_sum(db, col, field, target, start, end):
    _c = MongoClient('localhost', 27017, maxPoolSize=10000)

    r = _c[db][col].aggregate(
        [{"$match": {field: {"$gt": start,
                             "$lt": end}}},
         {"$group": {"_id": None, target: {"$sum": f"${target}"}}}]
    )
    return list(r)[0][target]


def _update_index(db, cols, field, drop=False):
    _c = MongoClient('localhost', 27017, maxPoolSize=10000)
    try:
        for c in cols:
            if drop:
                _c[db][c].drop_index(str(field) + "_1")
            else:
                _c[db][c].create_index(field)
    except Exception as e:
        print(e)


def _search(db, col, field, value, sort_head):
    _c = MongoClient('localhost', 27017, maxPoolSize=10000)
    if sort_head:
        return list(_c[db][col].find({field: value}).sort("ts").limit(1000))
    return list(_c[db][col].find({field: value}))


def main():
    Benchmark().connect().run()


if __name__ == '__main__':
    main()
