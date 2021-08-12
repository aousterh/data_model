#!/usr/bin/env python3

import os
import sys

import yaml
import tempfile
import glob
import pandas as pd

import util
from util import timed, path_join


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
        os.system(f"cp -r {src}/* {d}")
        os.system(f"cd {d}; gzip -d *.gz")
        fs = glob.glob(path_join(d, "*.ndjson"))

        conn = util.db_conn(use_sqlalchemy=True)
        print("connected to postgres")

        for i, f in enumerate(fs):
            print(f"loading dataframes {i + 1}/{len(fs)}")
            df = pd.read_json(f, lines=True)
            name = os.path.basename(f).replace(".ndjson", "")
            df.to_sql(name, con=conn, if_exists='replace', index=True, method='multi')


if __name__ == '__main__':
    load()
