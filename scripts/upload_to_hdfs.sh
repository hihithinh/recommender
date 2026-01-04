#!/bin/bash

# Upload raw CSV files to HDFS
# Run this script on the master machine after starting HDFS

echo "Creating HDFS directories..."
docker exec namenode hadoop fs -mkdir -p /data/raw
docker exec namenode hadoop fs -mkdir -p /data/processed
docker exec namenode hadoop fs -mkdir -p /data/embeddings
docker exec namenode hadoop fs -mkdir -p /data/models

echo "Uploading CSV files to HDFS..."
docker cp ./data/raw/BX-Book-Ratings.csv namenode:/tmp/
docker cp ./data/raw/BX-Users.csv namenode:/tmp/
docker cp ./data/raw/BX_Books.csv namenode:/tmp/

docker exec namenode hadoop fs -put -f /tmp/BX-Book-Ratings.csv /data/raw/
docker exec namenode hadoop fs -put -f /tmp/BX-Users.csv /data/raw/
docker exec namenode hadoop fs -put -f /tmp/BX_Books.csv /data/raw/

echo "Verifying files in HDFS..."
docker exec namenode hadoop fs -ls /data/raw/

echo "Done! Files uploaded to HDFS."
