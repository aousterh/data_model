# 1 script that reads in the data, finds the id.orig_h, generates the stream of random queries
# (which can be input to different backends besides elastic).
# This will be the workload generation script.
import json
import os
import pandas as pd
import random

def getUnique(field):
    recordList =[]
    recordDict = {}
    possibleVals= []
    df = pd.DataFrame()
    directory = os.getcwd()
    for filename in os.listdir(directory):
        if filename.endswith(".ndjson"):
            with open(filename) as f:
                for jsonObj in f:
                    recordDict = json.loads(jsonObj)
                    recordList.append(recordDict)
                for i, record in enumerate(recordList):
                    try:
                        possibleVals.append(recordList[i][field])
                    except KeyError as e:
                        pass
    df[field] = list(set(possibleVals))
    outfile = 'output/possibleVals.csv'
    with open(outfile, 'w') as outfile:
        df.to_csv(outfile, index=False, mode='a')
    return df[field]

def generateWorkload(query_name, field="id.orig_h" ,runs=1000):
    data = {}
    data['workload'] = []
    uniqueVals = getUnique(field)
    for i in range(runs):
        uniqueVal = random.choice(uniqueVals)
        data['workload'].append({'query': query_name + " " + field, 'arguments': [uniqueVal]})

    with open('output/workload.json', 'w') as outfile:
        json.dump(data, outfile, indent=4)

def main():
    os.system("mkdir output")
    generateWorkload("search", "id.orig_h", 1000)


if __name__ == "__main__":
    main()
