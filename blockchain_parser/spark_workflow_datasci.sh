#!/bin/sh
files="blockchain_data/blk*.dat"
echo "###### Spark Parsing Workflow Starts... ######"
spark-submit \
  --num-executors 4 --executor-cores 4 --executor-memory 24G --driver-memory 4g \
  my_parser.py $files &&

hadoop fs -cat csv/transactions/part* | sed -e '/^ *$/d' | hadoop fs -put - transactions.csv &&
hadoop fs -rm -r csv/transactions &&
hadoop fs -cat csv/addrs/part* | sed -e '/^ *$/d' | hadoop fs -put - addrs.csv &&
hadoop fs -rm -r csv/addrs
echo "\n###### Spark Parsing Workflow Finishes. ######\n"
