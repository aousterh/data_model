#!/bin/bash

printf "Search Query 1\n"
printf "find all records with IP 10.128.0.19\n"
curl -X GET 'http://localhost:9200/test/_search?q=id.orig_h:10.128.0.19&pretty=true'

printf "\nSearch Query 2\n"
printf "find all records with IP 10.128.0.19, sort by timestamp\n"
curl -X GET "localhost:9200/test/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "sort" : ["ts" ],
  "query" : {
    "term" : { "id.orig_h" : "10.128.0.19" }
  }
}
'

printf "\nSearch Query 3\n"
printf "find all records with IP 10.128.0.19, sort by timestamp, and return the first 5\n"
curl -X GET "localhost:9200/test/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 5,
  "sort" : ["ts" ],
  "query" : {
    "term" : { "id.orig_h" : "10.128.0.19" }
  }
}
'