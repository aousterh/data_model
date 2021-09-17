#!/bin/bash
# script to mount a volume and copy its data to the local SSD

MOUNT_DIR=/remote_volume
DEVICE_NAME=nvme1n1
LOCAL_DIR=/local/zeek-data-all
DATA_DIR=subset_80_million/ndjson-nested

# create directory and mount volume
sudo mkdir -p $MOUNT_DIR
sudo chown admin $MOUNT_DIR
sudo mount $DEVICE_NAME $MOUNT_DIR

# make local directory for data, copy it over in parallel, and wait for the
# copies to complete
mkdir -p $LOCAL_DIR/$DATA_DIR
for file in $MOUNT_DIR/$DATA_DIR/*
do
    time cp "$file" $LOCAL_DIR/$DATA_DIR/"$(basename "$file")" &
done
wait < <(jobs -p)


echo "The following command should return 34671.7, indicating that all the data was copied."
ls -l $LOCAL_DIR/$DATA_DIR/* | awk '{sum+=$5;} END {print sum/(1024*1024);}'
