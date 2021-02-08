#!/bin/bash

ZNG_PATH=../../zq-sample-data/zng-uncompressed/*.zng

printf "Analytics query\n"
printf "count total number of records with each distinct source IP\n"
zq -t 'count() by id.orig_h' $ZNG_PATH

printf "\nSearch query\n"
printf "find all records with IP 10.128.0.19, sort by timestamp, and return the first 5\n"
zq -t 'id.orig_h=10.128.0.19 | sort ts | head 5' $ZNG_PATH

printf "\nData discovery query\n"
printf "count the number of records with each different schema\n"
zq -t 'count() by typeof(.)' $ZNG_PATH
