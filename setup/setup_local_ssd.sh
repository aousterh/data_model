#!/bin/bash
# script to mount a volume and copy its data to the local SSD

REMOTE_DIR=/remote_volume
REMOTE_DEVICE=nvme3n1
LOCAL_DIR=/local/zeek-data-all
LOCAL_DEVICE=nvme2n1
DATA_DIR=subset_80_million/ndjson-nested

# create directory and mount remote volume
sudo mkdir -p $REMOTE_DIR
sudo chown admin $REMOTE_DIR
sudo mount /dev/$REMOTE_DEVICE $REMOTE_DIR

# create directory, set up file system on local SSD and mount it
sudo mkdir -p $LOCAL_DIR
sudo mkfs -t xfs /dev/$LOCAL_DEVICE
sudo mount /dev/$LOCAL_DEVICE $LOCAL_DIR
sudo chown -R admin $LOCAL_DIR

# copy data over in parallel, and wait for the copies to complete
echo "copying data"
mkdir -p $LOCAL_DIR/$DATA_DIR
for file in $REMOTE_DIR/$DATA_DIR/*
do
    time cp "$file" $LOCAL_DIR/$DATA_DIR/"$(basename "$file")" &
done
wait < <(jobs -p)

echo "The following commands should return the same amount, indicating that all the data was copied."
ls -l $REMOTE_DIR/$DATA_DIR/* | awk '{sum+=$5;} END {print sum/(1024*1024);}'
ls -l $LOCAL_DIR/$DATA_DIR/* | awk '{sum+=$5;} END {print sum/(1024*1024);}'

# unmount the remote volume
sudo umount $REMOTE_DIR
