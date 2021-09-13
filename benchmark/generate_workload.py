# Script for generating streams of random queries
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
BASE_DIR="/local/zeek-data-all/subset_80_million"
ZNG_DIR="/zq-sample-data/z/zng"
OUTPUT_DIR="workload/trace"
OUTPUT_FILENAMES = {
    "search": "{}/network_log_search_{}.ndjson",
    "analytics": "{}/network_log_analytics_{}.ndjson",
    "analytics avg": "{}/network_log_analytics_avg_{}.ndjson",
}

def getUnique(field):
    recordList =[]
    possibleVals= []
    df = pd.DataFrame()
    directory = BASE_DIR
    for filename in os.listdir(directory):
        # id_orig_h.ndjson includes a single column containing all unique
        # id.orig_h fields from the data
        if filename.endswith("id_orig_h.ndjson"):
            with open("{}/{}".format(BASE_DIR, filename)) as f:
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

def getNumericFieldFrequencies():
    freqs = {}

    for filename in os.listdir(BASE_DIR):
        # type_samples.ndjson includes a sample value of each different type
        if filename.endswith("type_samples.ndjson"):
            with open("{}/{}".format(BASE_DIR, filename)) as f:
                for jsonObj in f:
                    record = json.loads(jsonObj)
                    for k, v in record.items():
                        # check if an int and not a bool (bools are a subtype of int)
                        if isinstance(v, int) and not isinstance(v, bool):
                            freqs[k] = freqs.get(k, 0) + 1
    return freqs

def generateAggregationWorkload(query_name, runs=1000, seed=42):
    freqs = getNumericFieldFrequencies()
    del freqs["version"] # sometimes numeric but sometimes a string

    with open(OUTPUT_FILENAMES["analytics avg"].format(OUTPUT_DIR, runs), 'w') as f:
        writer = ndjson.writer(f, ensure_ascii=False)
        for i in range(runs):
            field = random.choices(list(freqs.keys()), list(freqs.values()))[0]
            writer.writerow({'query': query_name, 'arguments': [field]})

def main(newUniqueRun=True):
    search_field = "orig_h"

    if newUniqueRun:
        uniqueVals = getUnique(search_field)
    else:
        uniqueVals = pd.read_csv('{}/{}'.format(OUTPUT_DIR, UNIQUE_VALS_FILE), delimiter='\n')["id.orig_h"].to_list()
    
    generateSearchWorkload("search id.orig_h", uniqueVals, search_field, 100)
#    generateAnalyticsWorkload("analytics sum orig_bytes", 5, 30)
    generateAggregationWorkload("analytics avg field", 100)

if __name__ == "__main__":
    main()
