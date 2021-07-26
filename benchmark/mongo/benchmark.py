#!/usr/bin/env python3

import os
import yaml
from multiprocessing import Pool

from pymongo import MongoClient

import util


class Benchmark:
    def __init__(self):
        _c_file = os.environ.get('CONFIG', 'default') + ".yaml"
        with open(_c_file) as f:
            self._config = yaml.load(f, Loader=yaml.Loader)
            self._meta = self._config.get("meta", {})
            self._benchmark = self._config.get("benchmark", {})

        self._db = os.environ['DB']
        self._cols = None

    def connect(self):
        _c = MongoClient('localhost', 27017, maxPoolSize=10000)
        self._cols = _c[self._db].list_collection_names()
        return self

    def run(self):
        if self._meta.get("warmup", True):
            pass

        for workload in self._benchmark:
            wc = util.workload_config(workload)

            for name, param in wc["query"].items():
                query_funcs = list()

                if wc["kind"] == "search":
                    for v in param["values"]:
                        def f():
                            pool = Pool(self._meta.get("num_thread", 1))
                            results = pool.starmap(_search, [(self._db, c, param["field"], v)
                                                             for c in self._cols])
                            return results
                        query_funcs.append(f)
                elif wc["kind"] == "analytics":
                    pass
                else:
                    raise NotImplemented

                for f in query_funcs:
                    r = util.benchmark(f, num_iter=self._meta.get("num_run", 1))
                    r["name"] = name
                    # TBD dump to log
                    print(r)


def _search(db, col, field, value):
    _c = MongoClient('localhost', 27017, maxPoolSize=10000)
    return list(_c[db][col].find({field: value}))


def main():
    Benchmark().connect().run()


if __name__ == '__main__':
    main()
