# 1 script that reads in the data, finds the id.orig_h, generates the stream of random queries
# (which can be input to different backends besides elastic).
# This will be the workload generation script.
import json
import ndjson
import os
import pandas as pd
import random

#DATA_DIR=os.getcwd()
DATA_DIR="/zq-sample-data/zeek-ndjson"
OUTPUT_DIR="workload/trace"

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
    df[field] = list(set(possibleVals))
    outfile = '{}/possibleVals.csv'.format(OUTPUT_DIR)
    
    with open(outfile, 'w') as outfile:
        df.to_csv(outfile, index=False, mode='a')
    return df[field]

def generateWorkload(query_name, field="id.orig_h", runs=1000, seed=42):
    workload = []
    uniqueVals = getUnique(field)
    random.seed(seed)
    for i in range(runs):
        uniqueVal = random.choice(uniqueVals)
        workload.append({'query': query_name + " " + field, 'arguments': [uniqueVal]})

    with open('{}/network_log_search_{}.ndjson'.format(OUTPUT_DIR, runs), 'w') as f:
        writer = ndjson.writer(f, ensure_ascii=False)
        for query in workload:
            writer.writerow(query)

def main():
    generateWorkload("search", "id.orig_h", 30)

if __name__ == "__main__":
    main()
