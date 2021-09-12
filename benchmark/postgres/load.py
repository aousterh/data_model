#!/usr/bin/env python3

import os
import sys

import yaml
import tempfile
import glob
import pandas as pd

# XXX modin doesn't work due to read_json has trouble
# reading in string
if os.environ.get("MODIN"):
    import modin.pandas as pd

import util
from util import timed, path_join

import ray
import multiprocessing

CHUNK_SIZE = 100 * 1000


@timed
def load():
    name = os.environ.get("DATASET", "network_log")
    f, loc = os.environ.get("FORMAT", "ndjson"), None

    # find source
    with open(util.dataset_info) as _f:
        di = yaml.load(_f, Loader=yaml.Loader)
        loc = di.get(name, {}).get(f, {}).get("location", None)
    if loc is None:
        sys.exit(1)
    src = path_join(util.workload_dir, loc)

    with tempfile.TemporaryDirectory() as d:
        if not os.environ.get("NOZIP"):
            os.system(f"cp -r {src}/* {d}")
            os.system(f"cd {d}; gzip -d *.gz || true")
            fs = glob.glob(path_join(d, "*.ndjson"))
        else:
            fs = glob.glob(path_join(src, "*.ndjson"))

        if os.environ.get("RAY"):
            _ = ray.get([_ray_import.remote(f) for f in fs])
        else:
            pool = multiprocessing.Pool(multiprocessing.cpu_count())
            pool.map(_import, fs)

        # conn = util.db_conn(use_sqlalchemy=True)
        # print("connected to postgres")
        # for i, f in enumerate(fs):
        #     print(f"loading dataframes {i + 1}/{len(fs)}; name: {f}")
        #     df = pd.read_json(f, lines=True)
        #     name = os.path.basename(f).replace(".ndjson", "")
        #     df.to_sql(name, con=conn,
        #               if_exists='replace',
        #               index=True,
        #               method='multi',
        #               chunksize=CHUNK_SIZE)


@ray.remote
def _ray_import(*args, **kwargs):
    _import(*args, *kwargs)


def _import(f):
    conn = util.db_conn(use_sqlalchemy=True)
    print("connected to postgres")
    print(f"loading dataframe from file: {f}")
    df = pd.read_json(f, lines=True)
    name = os.path.basename(f).replace(".ndjson", "").split("_")[0]
    df.to_sql(name, con=conn,
              if_exists='append',
              index=True,
              method='multi',
              chunksize=CHUNK_SIZE)


if __name__ == '__main__':
    if os.environ.get("RAY"):
        ray.init()
    load()
