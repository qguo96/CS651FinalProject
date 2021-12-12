# reference: https://github.com/tenthirtyone/blocktools, modified to fit our blockchain data format

from my_blocktools import *


class my_BlockHeader:
    def __init__(self, blockchain):
        self.version = my_uint4(blockchain)
        self.previousHash = my_hash32(blockchain)
        self.merkleHash = my_hash32(blockchain)
        self.time = my_uint4(blockchain)
        self.bits = my_uint4(blockchain)
        self.nonce = my_uint4(blockchain)

    def toString(self):
        pass
        # print("Version:\t %d" % self.version)
        # print("Previous Hash\t %s" % hashStr(self.previousHash))
        # print("Merkle Root\t %s" % hashStr(self.merkleHash))
        # print("Time stamp\t "+ self.decodeTime(self.time))
        # print("Difficulty\t %d" % self.bits)
        # print("Nonce\t\t %s" % self.nonce)



class my_Block:
    def __init__(self, blockchain):
        self.blockHeader = my_BlockHeader(blockchain)
        self.txCount = my_varint(blockchain)
        self.Txs = []

        for i in range(0, self.txCount):
            tx = my_Tx(blockchain)
            self.Txs.append(tx)

    def toString(self):
        pass
        # print("")
        # print("#"*10 + " Block Header " + "#"*10)
        # self.blockHeader.toString()
        # print("")
        # print("##### Tx Count: %d" % self.txCount)
        # for t in self.Txs:
        #     t.toString()
        # print("#### end of all %d transactins" % self.txCount)

    def toMemory(self):
        inputrows = []
        outputrows = []
        txrows = []
        timestamp = blktime2datetime(self.blockHeader.time)
        # if timestamp.startswith('2013-10-25'):
        for tx in self.Txs:
            for m, input in enumerate(tx.inputs):
                inputrows.append((hashStr(tx.hash),
                                  m,
                                  hashStr(input.prevhash),
                                  input.txOutId,
                                  timestamp))
            amount = 0
            for n, output in enumerate(tx.outputs):
                amount += output.value
                outputrows.append((hashStr(tx.hash),
                                   n,
                                   output.value,
                                   rawpk2addr(output.pubkey)))
            txrows.append((hashStr(tx.hash),
                           amount,
                           timestamp,
                           tx.inCount,
                           tx.outCount))
        return inputrows, outputrows, txrows


class my_Tx:
    def __init__(self, blockchain):
        self.pos_start = blockchain.tell()

        self.version = my_uint4(blockchain)
        self.inCount = my_varint(blockchain)
        self.inputs = []
        for i in range(0, self.inCount):
            input = my_txInput(blockchain)
            self.inputs.append(input)
        self.outCount = my_varint(blockchain)
        self.outputs = []
        if self.outCount > 0:
            for i in range(0, self.outCount):
                output = my_txOutput(blockchain)
                self.outputs.append(output)
        self.lockTime = my_uint4(blockchain)

        self.pos_end = blockchain.tell()
        blockchain.seek(self.pos_start)
        self.hash = double_sha256(my_read(blockchain, (self.pos_end-self.pos_start)//2))[::-1]

    def toString(self):
        pass
        # print("")
        # print("="*20 + " No. %s " %self.seq + "Transaction " + "="*20)
        # print("Tx Version:\t %d" % self.version)
        # print("Inputs:\t\t %d" % self.inCount)
        # for i in self.inputs:
        #     i.toString()
        #
        # print("Outputs:\t %d" % self.outCount)
        # for o in self.outputs:
        #     o.toString()
        # print("Lock Time:\t %d" % self.lockTime)

        # print("self.hash is: {}".format(hashStr(self.hash)))
        # exit(651)


class my_txInput:
    def __init__(self, blockchain):
        self.prevhash = my_hash32(blockchain)
        self.txOutId = my_uint4(blockchain)
        self.scriptLen = my_varint(blockchain)

        # normal read will give error, use my_read instead
        # self.scriptSig = blockchain.read(self.scriptLen)
        self.scriptSig = my_read(blockchain, self.scriptLen)
        self.seqNo = my_uint4(blockchain)

    def toString(self):
        pass
        # print "\tPrev. Tx Hash:\t %s" % hashStr(self.prevhash)
        # print("\tTx Out Index:\t %s" % self.decodeOutIdx(self.txOutId))
        # print("\tScript Length:\t %d" % self.scriptLen)
		# print "\tScriptSig:\t %s" %
        # self.decodeScriptSig(self.scriptSig)
        # print("\tSequence:\t %8x" % self.seqNo)



class my_txOutput:
    def __init__(self, blockchain):
        self.value = my_uint8(blockchain)
        self.scriptLen = my_varint(blockchain)
        # self.pubkey = blockchain.read(self.scriptLen)
        self.pubkey = my_read(blockchain, self.scriptLen)

    def toString(self):
        pass
        # print("\tValue:\t\t %d" % self.value + " Satoshi")
        # print("\tScript Len:\t %d" % self.scriptLen)
        # print("\tScriptPubkey:\t %s" % self.decodeScriptPubkey(self.pubkey))
        #
        # print("self.pubkey is: {}".format(hashStr(self.pubkey)))
        # print("output addr is: %s" % rawpk2addr(self.pubkey))
        # exit(651)



