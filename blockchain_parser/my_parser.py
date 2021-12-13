# reference: https://github.com/pw2393/spark-bitcoin-parser, modified so that only one pyspark job is needed

import findspark
findspark.init("/Users/yifanzhang/server/spark-3.2.0-bin-hadoop3.2")
from pyspark import SparkConf, SparkContext
from pyspark import StorageLevel
from my_block import *
import shutil


output_folder = './csv/'
import os

if os.path.exists(output_folder):
    shutil.rmtree(output_folder)
os.makedirs(output_folder)

def parse(blockchain, blockname):
    print("")
    print("parsing: {}".format(blockname))
    print("")
    block = my_Block(blockchain)
    return block.toMemory()

def one_to_one_tx(two_lists):
    inputs = two_lists[0]
    outputs = two_lists[1]
    if len(inputs) == 0 or len(outputs) == 0:
        return []

    sum_in = 0
    # i = (val, addr, timestamp)
    # o = (val, addr)
    for i in inputs:
        sum_in += i[0]

    result = []
    for i in inputs:
        for o in outputs:
            if o[0] != 0: # Insidely filter out all zero output
                result.append(((i[1], o[1]), i[0]*o[0]*1.0/sum_in, i[2]))

    return result


def main():
    import sys
    if len(sys.argv) < 2:
        print("\n\tUSAGE:\n\t\tspark-submit spark_parser.py filename1.dat filename2.dat ...")
        return

    conf = SparkConf().setAppName("BTC-Parser")
    sc = SparkContext(conf=conf)

    rawfiles = sc.parallelize([sys.argv[i] for i in range(1, len(sys.argv))])
    blocks = rawfiles.map(lambda filename: parse(open(filename, "rb"), filename)).persist(StorageLevel.MEMORY_AND_DISK)

    # key: (prev_out_txhash, prev_out_index)
    # value: (tx_hash, input_index, tx_time)
    inputs = blocks.map(lambda x: x[0]) \
                   .flatMap(lambda x: x) \
                   .map(lambda txinrow: ((txinrow[2], str(txinrow[3])), (txinrow[0], str(txinrow[1]), txinrow[4])))

    # key: (tx_hash, output_index)
    # value: (output_volume, output_addr)
    outputs = blocks.map(lambda x: x[1]) \
                    .flatMap(lambda x: x) \
                    .map(lambda txoutrow: ((txoutrow[0], str(txoutrow[1])), (str(txoutrow[2]), txoutrow[3])))


    # tx_hash, tx_volume, tx_time, tx_incout, tx_outcount
    transactions = blocks.map(lambda x: x[2]) \
                         .flatMap(lambda x: x) \
                         .map(lambda txrow: txrow[0] + "," + str(txrow[1]) + "," + txrow[2] + "," + str(txrow[3]) + "," + str(txrow[4]))
    transactions.saveAsTextFile(output_folder+'transactions')

    # ( (tx_hash, input_index, tx_time), (input_volume, input_addr) )
    input_with_addr = inputs.join(outputs).values()
    # input_with_addr.map(lambda x: x[0][0] + "," + x[0][1] + "," + x[1][0] + "," + x[1][1] + "," + x[0][2]).saveAsTextFile(
    #     output_folder + "inputs")

    # key: tx_hash
    # value: (input_volume, input_addr, tx_time)
    new_inputs = input_with_addr.map(lambda x: (x[0][0], (float(x[1][0]), x[1][1], x[0][2])))

    # key: tx_hash
    # value: (output_volume, output_addr)
    new_outputs = outputs.map(lambda x: (x[0][0], (float(x[1][0]), x[1][1])))

    # input_addr, output_addr, tx_volume, tx_hash, tx_time
    addr = new_inputs.cogroup(new_outputs) \
                     .filter(lambda x: len(x[1][0]) != 0 and len(x[1][1]) != 0) \
                     .flatMapValues(one_to_one_tx)
    addr.map(lambda x:x[1][0][0]+","+x[1][0][1]+","+str(x[1][1])+","+x[0]+","+x[1][2]).saveAsTextFile(output_folder+"addrs")


if __name__ == "__main__":

    import time
    start_time = time.time()

    main()

    print("--- %s seconds ---" % (time.time() - start_time))




