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

# for conn we need to start with data with tunnel parents
# for http we need info_code, info_msg, orig_filenames, and proxied. I couldn't find any records with all so
# I modified the first record in this file to include them all
# not sure what the problem field was for dns, files, or ssl
INIT_FILES=["/local/zeek-data-all/subset_80_million/zeek-ndjson-fused-chunks/json_streaming_connzita.ndjson",
            "/local/zeek-data-all/subset_80_million/zeek-ndjson-fused-chunks/json_streaming_httpbj.ndjson",
            "/local/zeek-data-all/subset_80_million/zeek-ndjson-fused-chunks/json_streaming_sslel.ndjson",
            "/local/zeek-data-all/subset_80_million/zeek-ndjson-fused-chunks/json_streaming_filesbu.ndjson",
            "/local/zeek-data-all/subset_80_million/zeek-ndjson-fused-chunks/json_streaming_dnsgk.ndjson",
            "/local/zeek-data-all/subset_80_million/zeek-ndjson-fused-chunks/json_streaming_x509be.ndjson"]

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

        # we need to make sure we load specific files first so that the tables
        # are created with the correct schema
        for f in INIT_FILES:
            _import(f)
        fs_2 = [f for f in fs if f not in INIT_FILES]
        print("num files: {} {}".format(len(fs), len(fs_2)))

        if os.environ.get("RAY"):
            _ = ray.get([_ray_import.remote(f) for f in fs_2])
        else:
            pool = multiprocessing.Pool(multiprocessing.cpu_count())
            pool.map(_import, fs_2)

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

    name = os.path.basename(f).split("json_streaming_")[1][:9]
    name = name[:3]
    if "smb_files" in os.path.basename(f):
        name = "smb_files"
    elif "smb_mapping" in os.path.basename(f):
        name = "smb_mapping"
    elif "notice" in os.path.basename(f):
        name = "notice"

    print("file: {}, table: {}".format(f, name))

    try:
        df.to_sql(name, con=conn,
                  if_exists='append',
                  index=True,
                  method='multi',
                  chunksize=CHUNK_SIZE)
    except:
        print("exception! working on file {} table {}".format(f, name))
        raise


if __name__ == '__main__':
    if os.environ.get("RAY"):
        ray.init()
    load()
