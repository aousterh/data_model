#!/usr/bin/env python3
import time
import numpy as np
import pandas as pd
import copy
from resource import getrusage as resource_usage, RUSAGE_SELF
from time import time as timestamp
from collections import defaultdict

files = ["../../zq-sample-data/zeek-ndjson/" + i
         for i in ["conn.ndjson",
                   "http.ndjson",
                   "files.ndjson",
                   "dns.ndjson",
                   "ssl.ndjson",
                   "weird.ndjson",
                   "syslog.ndjson",
                   "rdp.ndjson",
                   "ntp.ndjson",
                   "smtp.ndjson"]
         ]


def unix_time(function, *args, **kwargs):
    '''Return `real`, `sys` and `user` elapsed time, like UNIX's command `time`
    You can calculate the amount of used CPU-time used by your
    function/callable by summing `user` and `sys`. `real` is just like the wall
    clock.
    Note that `sys` and `user`'s resolutions are limited by the resolution of
    the operating system's software clock (check `man 7 time` for more
    details).
    '''
    start_time, start_resources = timestamp(), resource_usage(RUSAGE_SELF)
    r = function(*args, **kwargs)
    end_resources, end_time = resource_usage(RUSAGE_SELF), timestamp()

    return {'return': r,
            'real': end_time - start_time,
            'sys': end_resources.ru_stime - start_resources.ru_stime,
            'user': end_resources.ru_utime - start_resources.ru_utime}


# TBD time load()
def load():
    return [pd.read_json(f, lines=True) for f in files]


def analytics(dfs):
    _field = "id.orig_h"
    pd.concat([f[[_field]] for f in dfs
               if _field in f.columns]) \
        .groupby(_field) \
        .size() \
        .reset_index(name='count')


def search_concat(dfs):
    _field = "id.orig_h"
    _dfs = pd.concat([f for f in dfs
                      if _field in f.columns], sort=False)
    _dfs[_dfs["id.orig_h"] == "10.128.0.19"].sort_values("ts").head(5)


def search(dfs):
    def _search(field, label, key, n):
        rows, results = list(), list()
        for f in dfs:
            if field in f.columns:
                rows += f[f[field] == label].iterrows()

        return sorted(rows, key=lambda x: x[1][key])[:n]

    _ = _search("id.orig_h", "10.128.0.19", "ts", 5)


def discovery(dfs):
    for df in dfs:
        _, _ = df.dtypes, len(df)


def benchmark(fn, dfs, num_iter=10):
    _real, _sys, _user = list(), list(), list()
    for _ in range(num_iter):
        _dfs = copy.deepcopy(dfs)
        t = unix_time(fn, dfs=_dfs)
        _real.append(t["real"])
        _sys.append(t["sys"])
        _user.append(t["user"])

    print(f"{fn.__name__},"
          f"{round(np.mean(_real), 5)},"
          f"{round(np.mean(_user), 5)},"
          f"{round(np.mean(_sys), 5)}")


def main():
    # TBD use sys.argv[0] for num_iter or dataset
    print("loading..")
    dfs = unix_time(load)["return"]

    print("name,real,user,sys")
    print("------------------")

    benchmark(analytics, dfs, num_iter=3)
    benchmark(search, dfs, num_iter=3)
    benchmark(discovery, dfs, num_iter=3)


if __name__ == '__main__':
    main()
