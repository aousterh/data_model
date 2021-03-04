#!/usr/bin/env python3
import time
import numpy as np
import pandas as pd
import copy


def timed(method):
    def timeit(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f s' % (method.__name__, (te - ts) * 1))
        return result

    return timeit


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


@timed
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


def benchmark(fn, num_iter=10):
    qcts = list()
    for _ in range(num_iter):
        _dfs = copy.deepcopy(dfs)
        start = time.time()
        fn(_dfs)
        qcts.append(time.time() - start)
    print(f"{fn.__name__},{round(np.mean(qcts), 5)},{round(np.std(qcts), 5)}")


def main():
    # TBD use sys.argv[0] for num_iter or dataset
    global dfs
    print("loading..")
    dfs = load()

    print("mean, std")
    print("---------")
    benchmark(analytics, num_iter=3)
    benchmark(search, num_iter=3)
    benchmark(discovery, num_iter=3)


if __name__ == '__main__':
    main()
