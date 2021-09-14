import json
import ndjson
import numpy as np
import os
import pandas as pd
import time

from elastic_benchmark_config import *

input_directory = DATA_DIR
workload_file = WORKLOAD_FILE_NAME
output_directory = OUTPUT_DIR
output_file = OUTPUT_FILE_NAME
queries = QUERIES
aggregation_fields = AGGREGATION_FIELDS

def runQuery(query):
    cmd = query + " > " + output_directory + "query-output.json"
    os.system(cmd)

    sortvals = []
    with open(output_directory + "query-output.json", 'r') as j:
        contents = json.loads(j.read())
        executiontime = contents["took"]
        hits = contents["hits"]["total"]["value"]
        ret = {"executiontime": executiontime * 0.001, "hits": hits, "sortval": 0}
        for content in contents["hits"]["hits"]:
            sortvals.append(content["sort"])
        if sortvals:
            sortval = sortvals.pop()[0]
            if sortval:
                ret["sortval"] = sortval
    os.system("rm " + output_directory + "query-output.json")
    return ret

def getQuery(queryname, arguments, sortval):
    # TODO: edit queries to return all results, not just top 10-1000
    if queryname == "search id.orig_h":
        if sortval:
            query = "curl -X GET \"localhost:9200/test/_search?pretty\" -H 'Content-Type: application/json' -d' { \"search_after\": ["+str(sortval)+"] , \"sort\": [{\"ts\": \"asc\"}],\"query\" : {\"term\" : {\"id.orig_h\" : \""+arguments[0]+"\" } } } '"
        else:
            query = "curl -X GET \"localhost:9200/test/_search?pretty\" -H 'Content-Type: application/json' -d' {  \"sort\": [{\"ts\": \"asc\"}], \"query\" : {\"term\" : { \"id.orig_h\" : \""+arguments[0]+"\" } } } '"
    #elif queryname == "search id.orig_h + sort ts":
    #     query = "curl -X GET \"localhost:9200/test/_search?format=json&pretty\" -H 'Content-Type: application/json' -d'{\"sort\" : [\"ts\" ],\"query\" : {\"term\" : { \"id.orig_h\" : \""+arguments[0]+"\" }}}'"
    elif queryname == "search id.orig_h + sort ts + slice 1000":
        query = "curl -X GET \"localhost:9200/test/_search?format=json&pretty\" -H 'Content-Type: application/json' -d'{\"size\": 1000,\"sort\" : [{\"ts\" : {\"order\": \"asc\" }}],\"query\" : {\"term\" : { \"id.orig_h\" : \""+arguments[0]+"\" }}}'"
    # elif queryname == "search id.orig_h + count by id.resp_h":
    #     query  = "curl -X GET \"localhost:9200/test/_search?format=json&pretty\" -H 'Content-Type: application/json' -d'{\"query\" : {\"term\" : { \"id.orig_h\" : \""+ arguments[0] +"\" }}, \"aggs\": {\"id.resp_h\": {\"terms\": {\"field\": \"id.resp_h\"}}}}'"
    # elif queryname == "search id.orig_h + sum orig_bytes":
    #     query = "curl -X GET \"localhost:9200/test/_search?format=json&pretty\" -H 'Content-Type: application/json' -d'{\"query\" : {\"term\" : { \"id.orig_h\" : \""+ arguments[0] +"\" }}, \"aggs\": {\"orig_bytes\": {\"sum\": {\"field\": \"orig_bytes\"}}}}'"
    # elif queryname == "search id.orig_h + count by schema":
    #     query = "curl -X GET \"localhost:9200/test/_search?format=json&pretty\" -H 'Content-Type: application/json' -d'{\"query\" : {\"term\" : { \"id.orig_h\" : \""+ arguments[0] +"\" }}, \"aggs\": {\"schema\": {\"terms\": {\"field\": \"_path\"}}}}'"
    elif queryname == "analytics avg field":
        if sortval:
            query = "curl -X GET \"localhost:9200/test/_search?pretty\" -H 'Content-Type: application/json' -d'{ \"sort\": [{\"ts\": \"asc\"}], \"search_after\": ["+str(sortval)+"], \"aggs\": {\"time\": {\"date_range\": {\"field\": \"ts\",\"ranges\": [{\"from\": \""+arguments[0]+"\", \"to\": \""+arguments[1]+"\"}]},\"aggs\": {\"avg_orig_bytes\": {\"avg\": { \"field\": \"orig_bytes\"}}}}}}'"
        else:
            query = "curl -X GET \"localhost:9200/test/_search?pretty\" -H 'Content-Type: application/json' -d'{ \"sort\": [{\"ts\": \"asc\"}], \"aggs\": {\"time\": {\"date_range\": {\"field\": \"ts\",\"ranges\": [{\"from\": \""+arguments[0]+"\", \"to\": \""+arguments[1]+"\"}]},\"aggs\": {\"avg_orig_bytes\": {\"avg\": { \"field\": \"orig_bytes\"}}}}}}'"
    else:
        query= ""
    return query

def issueQueries():
    took_list = []
    query_name_list = []
    argument_list = []
    validation_list =[]
    sortval_list=[]
    real_list=[]

    restartElastic()


    with open(input_directory + workload_file ) as f:
        data = ndjson.load(f)
        for d in data:
            start_time = time.time()
            if (d['query'] in queries):
                queryname = d['query']
                query = getQuery(queryname, d['arguments'], 0)
                if query:
                    if queryname in aggregation_fields:
                        prepForAggregation(aggregation_fields[queryname])

                    queryoutput = runQuery(query)
                    sortval = queryoutput['sortval']
                    indiv_hits = queryoutput['hits']

                    while indiv_hits >= 10000 and sortval:
                        sa_query = getQuery(queryname, d['arguments'], sortval)
                        curr_queryoutput = runQuery(sa_query)

                        queryoutput["executiontime"] = queryoutput["executiontime"] + curr_queryoutput["executiontime"]
                        queryoutput["hits"] = queryoutput["hits"] + curr_queryoutput["hits"]
                        queryoutput["sortval"] = curr_queryoutput["sortval"]

                        sortval = curr_queryoutput["sortval"]
                        indiv_hits = curr_queryoutput["hits"]

                    took_list.append(queryoutput["executiontime"])
                    query_name_list.append(d['query'])
                    argument_list.append(d['arguments'])
                    validation_list.append(queryoutput["hits"])
                    sortval_list.append(queryoutput["sortval"])

                    if queryname in aggregation_fields:
                        cleanFromAggregation(aggregation_fields[queryname])
                else:
                    print('Query Not Supported: ' + str(d['query']))
            else:
                print('Query Not Supported: ' + str(d['query']))

            end_time = time.time()
            real_list.append(end_time-start_time)

    num_queries = len(took_list)
    output_df = pd.DataFrame({'index': range(num_queries)})
    output_df = output_df.set_index('index')

    output_df.insert(0, 'system', ['Elastic'] * num_queries)
    output_df.insert(1, 'in_format', ['index'] * num_queries)
    output_df.insert(2,'out_format', ['elastic'] * num_queries)
    output_df.insert(3, 'query', query_name_list)
    output_df.insert(4,'start_time', [0] * num_queries)
    output_df.insert(5,'real', real_list)
    output_df.insert(6, 'user', [np.nan] * num_queries)
    output_df.insert(7, 'sys',[np.nan] * num_queries)
    output_df.insert(8, 'argument_0', argument_list)
    output_df.insert(9, 'validation', validation_list)
    output_df.insert(10, 'instance', ["m5.large"] * num_queries)
    output_df.insert(11,'took', took_list)
    #output_df.insert(11, 'sortval', sortval_list)



    output_df.to_csv(output_directory + output_file, na_rep='NaN')
    return output_df


def restartElastic():
    os.system("sudo systemctl stop elasticsearch.service")
    print("Elastic Stopped")
    os.system("sudo systemctl start elasticsearch.service")
    print("Elastic Started")
    os.system("sleep 30")
    print("Sleep Complete")


# Aggregation prep/clean due to:
# "Text fields are not optimised for operations that require per-document field data like aggregations and sorting, so these operations are disabled by default.
# Please use a keyword field instead.
# Alternatively, set fielddata=true on [id.resp_h] in order to load field data by uninverting the inverted index.
# Note that this can use significant memory."

def prepForAggregation(fieldname):
    prep_query = "curl -X PUT \"localhost:9200/test/_mapping/_doc?include_type_name=true&pretty\" -H 'Content-Type: application/json' -d'{\"properties\": { \""+ fieldname +"\" : { \"type\": \"text\", \"fielddata\": true}}}'"
    os.system(prep_query)

def cleanFromAggregation(fieldname):
    clean_query = "curl -X PUT \"localhost:9200/test/_mapping/_doc?include_type_name=true&pretty\" -H 'Content-Type: application/json' -d'{\"properties\": { \""+ fieldname +"\" : { \"type\": \"text\", \"fielddata\": false}}}'"
    os.system(clean_query)

def main():
    df = issueQueries()
    print df

if __name__ == "__main__":
    main()
