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

        for workload in self._benchmark:
            wc = util.workload_config(workload)

            for name, param in wc["query"].items():
                start = time.time()
                results = list()
                query_funcs = list()

                _update_index(self._db, self._cols, param["field"],
                              drop=not param.get("index", False))

                if wc["kind"] == "search":
                    def make_exec(_v):
                        def _exec():
                            if len(self._cols) > 1:
                                pool = Pool(self._meta.get("num_thread", 1))
                                _output = pool.starmap(_search, [(self._db, c, param["field"], _v)
                                                                 for c in self._cols])
                            else:
                                _output = _search(self._db, self._cols[0], param["field"], _v)
                            return _output

                        return _exec

                    values = list()
                    tf = param.get("trace_file", None)
                    if tf:
                        values = [row["arguments"][0] for row in util.read_trace(tf)]
                        values = values[:param.get("batch_size", len(values))]
                    for v in values:
                        query_funcs.append((make_exec(v), v))

                elif wc["kind"] == "analytics":
                    # TBD
                    def make_exec(_s, _e):
                        def _exec():
                            if len(self._cols) > 1:
                                pool = Pool(self._meta.get("num_thread", 1))
                                _output = pool.starmap(_range_sum, [(self._db, c,
                                                                     param["field"],
                                                                     param["target"], _s, _e)
                                                                    for c in self._cols])
                            else:
                                _output = _range_sum(self._db, self._cols[0],
                                                     param["field"],
                                                     param["target"], _s, _e)
                            return _output

                        return _exec

                    values = list()
                    tf = param.get("trace_file", None)
                    if tf:
                        values = [row["arguments"] for row in util.read_trace(tf)]
                        values = values[:param.get("batch_size", len(values))]
                    for _start, _end in values:
                        query_funcs.append((make_exec(_start, _end), f"{_start} {_end}"))
                else:
                    raise NotImplemented

                for i, (f, arg) in enumerate(query_funcs):
                    print(f"progress: running with {i + 1}/{len(query_funcs)}")
                    r = util.benchmark(f, num_iter=self._meta.get("num_run", 1))

                    # dump to log
                    results.append(OrderedDict({
                        "index": i,
                        "system": "mongo",
                        "in_format": "index" if param.get("index", False) else "bson",
                        "out_format": "json",
                        "query": param["desc"],
                        "start_time": round(time.time() - start, 3),
                        "real": r["real"],
                        "user": r["user"],
                        "sys": r["sys"],
                        "argument_0": arg,
                        "validation": len(r["return"]) if wc["kind"] == "search" else r["return"],
                    }))
            util.write_csv(results, f"mongo-{wc['kind']}-{name}.csv")


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
                _c[db][c].drop_index(field)
            else:
                _c[db][c].create_index(field)
    except:
        pass


def _search(db, col, field, value):
    _c = MongoClient('localhost', 27017, maxPoolSize=10000)
    return list(_c[db][col].find({field: value}))


def main():
    Benchmark().connect().run()


if __name__ == '__main__':
    main()
