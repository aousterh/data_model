#!/usr/bin/env python3

import os
import sys

import yaml
import tempfile
import glob

import util
from util import timed, path_join

import multiprocessing


@timed
def load():
    name = os.environ.get("DATASET", "network_log")
    f, loc = os.environ.get("FORMAT", "ndjson-nested"), None

    # find source
    with open(util.dataset_info) as _f:
        di = yaml.load(_f, Loader=yaml.Loader)
        loc = di.get(name, {}).get(f, {}).get("location", None)
    if loc is None:
        sys.exit(1)
    src = path_join(util.workload_dir, loc)

    with tempfile.TemporaryDirectory() as d:
        os.system(f"cp -r {src}/* {d}")
        os.system(f"cd {d}; gzip -d *.gz || true; "
                  f"echo 'ignored; assume uncompressed data'")
        fs = glob.glob(path_join(d, "*.ndjson"))

        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        pool.map(_import, fs)

        # col = os.environ.get("COL", "zed")
        # for f in fs:
        #     os.system(f"mongoimport -d "
        #               f"{os.environ['DB']} "
        #               f"{f'-c {col} ' if col is not None else ''}"
        #               f"{f}")


def _import(f):
    col = os.environ.get("COL", "zed")
    os.system(f"mongoimport -d "
              f"{os.environ['DB']} "
              f"{f'-c {col} ' if col is not None else ''}"
              f"{f}")


if __name__ == '__main__':
    load()
