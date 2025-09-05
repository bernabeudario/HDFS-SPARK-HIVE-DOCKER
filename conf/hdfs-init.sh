#!/bin/bash
until hdfs dfsadmin -safemode get | grep -q OFF; do
  echo "Waiting for HDFS safemode to turn off..."
  sleep 5
done
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -chmod 777 /spark-logs
hdfs dfs -mkdir -p /aux
hdfs dfs -chmod 777 /aux
echo "Folders created in HDFS."