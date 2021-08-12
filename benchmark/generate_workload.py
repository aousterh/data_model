# 1 script that reads in the data, finds the id.orig_h, generates the stream of random queries
# (which can be input to different backends besides elastic).
# This will be the workload generation script.
from datetime import timedelta
from dateutil import parser
import glob
import json
import ndjson
import os
import pandas as pd
import random

#DATA_DIR=os.getcwd()
DATA_DIR="/zq-sample-data/zeek-ndjson"
ZNG_DIR="/zq-sample-data/z/zng"
OUTPUT_DIR="workload/trace"
OUTPUT_FILENAMES = {
    "search": "{}/network_log_search_{}.ndjson",
    "analytics": "{}/network_log_analytics_{}.ndjson"
}

def getUnique(field):
    recordList =[]
    possibleVals= []
    df = pd.DataFrame()
    directory = DATA_DIR
    for filename in os.listdir(directory):
        if filename.endswith(".ndjson"):
            with open("{}/{}".format(DATA_DIR, filename)) as f:
                for jsonObj in f:
                    record = json.loads(jsonObj)
                    try:
                        possibleVals.append(record[field])
                    except KeyError as e:
                        pass
    df[field] = possibleVals
    outfile = '{}/possibleVals.csv'.format(OUTPUT_DIR)
    
    with open(outfile, 'w') as outfile:
        df.to_csv(outfile, index=False, mode='a')
    return df[field]

def generateSearchWorkload(query_name, uniqueVals, field="id.orig_h", runs=1000, seed=42):
    workload = []
    random.seed(seed)
    for i in range(runs):
        uniqueVal = random.choice(uniqueVals)
        workload.append({'query': query_name , 'arguments': [uniqueVal]})

    with open(OUTPUT_FILENAMES["search"].format(OUTPUT_DIR, runs), 'w') as f:
        writer = ndjson.writer(f, ensure_ascii=False)
        for query in workload:
            writer.writerow(query)

def getTimestampBounds():
    # use zq to find the min and max timestamps in our data
    os.system("zq -f json -o tmp.json 'min(ts)' {}/*.zng".format(ZNG_DIR))
    with open("tmp.json") as f:
        min_ts = parser.parse(json.loads(f.readline())["min"])

    os.system("zq -f json -o tmp.json 'max(ts)' {}/*.zng".format(ZNG_DIR))
    with open("tmp.json") as f:
        max_ts = parser.parse(json.loads(f.readline())["max"])

    os.system("rm tmp.json")

    return (min_ts, max_ts)

def generateAnalyticsWorkload(query_name, window_size_s=5, runs=1000, seed=42):
    random.seed(seed)
    (min_ts, max_ts) = getTimestampBounds()

    with open(OUTPUT_FILENAMES["analytics"].format(OUTPUT_DIR, runs), 'w') as f:
        writer = ndjson.writer(f, ensure_ascii=False)
        for i in range(runs):
            offset = random.random() * ((max_ts - min_ts).total_seconds() - window_size_s)
            window_start = min_ts + timedelta(seconds=offset)
            args = [window_start, window_start + timedelta(seconds=window_size_s)]
            writer.writerow({'query': query_name, 'arguments':
                             [t.strftime("%Y-%m-%dT%H:%M:%S.%fZ") for t in args]})

def main(newUniqueRun=False):
    if newUniqueRun:
        uniqueVals = getUnique("id.orig_h")
    else:
        uniqueVals = pd.read_csv('{}/{}'.format(OUTPUT_DIR, UNIQUE_VALS_FILE), delimiter='\n')["id.orig_h"].to_list()
    
    generateSearchWorkload("search id.orig_h", uniqueVals, "id.orig_h", 30)
    generateAnalyticsWorkload("analytics sum orig_bytes", 5, 30)

if __name__ == "__main__":
    main()
