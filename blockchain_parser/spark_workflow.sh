#!/bin/sh
files="/Users/yifanzhang/Documents/Go_Workspace/src/bitcoin_blockchain_downloading/blockchain/RAW/blk*.dat"
echo "###### Spark Parsing Workflow Starts... ######"
/Users/yifanzhang/server/spark-3.2.0-bin-hadoop3.2/bin/spark-submit \
  --master local[*] \
  my_parser.py $files &&
# Better Data Storage:
cat csv/transactions/part* | sed -e '/^ *$/d' > csv/transactions.csv &&
rm -r csv/transactions &&
cat csv/addrs/part* | sed -e '/^ *$/d' > csv/addrs.csv &&
rm -r csv/addrs
echo "\n###### Spark Parsing Workflow Finishes. ######\n"
