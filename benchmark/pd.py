#!/usr/bin/env python3
import pandas as pd
import copy
from collections import defaultdict
from util import benchmark, unix_time

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

# TBD time load()
def load():
    return [pd.read_json(f, lines=True) for f in files]


def init_pd(dfs):
    _dfs = copy.deepcopy(dfs)
    return ([_dfs], {})


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

def main():
    # TBD use sys.argv[0] for num_iter or dataset
    print("loading..")
    dfs = unix_time(load)["return"]

    print("name,real,user,sys")
    print("------------------")

    benchmark(analytics, init_pd, dfs, num_iter=3)
    benchmark(search, init_pd, dfs, num_iter=3)
    benchmark(search_concat, init_pd, dfs, num_iter=3)
    benchmark(discovery, init_pd, dfs, num_iter=3)


if __name__ == '__main__':
    main()
