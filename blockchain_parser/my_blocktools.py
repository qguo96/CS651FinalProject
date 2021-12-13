# reference: https://github.com/tenthirtyone/blocktools, modified to fit our blockchain data format

import struct
from my_base58 import *


def my_uint1(stream):
    return ord(bytes.fromhex(str(stream.read(2).decode("utf-8"))))


def my_uint2(stream):
    tmp = bytes.fromhex(str(stream.read(4).decode("utf-8")))
    return struct.unpack('H', tmp)[0]


def my_uint4(stream):
    tmp = bytes.fromhex(str(stream.read(8).decode("utf-8")))
    return struct.unpack('I', tmp)[0]


def my_uint8(stream):
    tmp = bytes.fromhex(str(stream.read(16).decode("utf-8")))
    return struct.unpack('Q', tmp)[0]


def my_hash32(stream):
    tmp = bytes.fromhex(str(stream.read(64).decode("utf-8")))
    return tmp[::-1]


def my_read(stream, size):
    true_size = 2*size
    tmp = bytes.fromhex(str(stream.read(true_size).decode("utf-8")))
    return tmp


def my_read_true_size(stream, true_size):
    tmp = bytes.fromhex(str(stream.read(true_size).decode("utf-8")))
    return tmp


def my_varint(stream):
    size = my_uint1(stream)
    # print("size is {}".format(size))

    if size < 0xfd:
        return size
    if size == 0xfd:
        return my_uint2(stream)
    if size == 0xfe:
        return my_uint4(stream)
    if size == 0xff:
        return my_uint8(stream)
    return -1


def hashStr(bytebuffer):
    return ''.join(('%02x'% a) for a in bytebuffer)


def rawpk2hash160(pk_script):
    """
    Locate the raw 20-byte hash160 value of a public key right after 0x14
    """
    head = pk_script.find(b'\x14') + 1
    # print("head is: {}".format(head))
    return pk_script[head:head + 20]


def rawpk2addr(pk_script):
    """
    Convert a raw bitcoin block script of public key to a common address
    """
    return hash_160_to_bc_address(rawpk2hash160(pk_script))


def blktime2datetime(blktime):
    """
    Convert a bitcoin block timestamp integer to a datetime string.
    Note that current timestamp as seconds since 1970-01-01T00:00 UTC.
    """
    from datetime import timedelta, datetime
    d = datetime(1970, 1, 1, 0, 0, 0) + timedelta(days=int(blktime) / 86400, seconds=int(blktime) % 86400)
    return d.strftime('%Y-%m-%dT%H:%M:%S')


def double_sha256(bytebuffer):
  """
  Dual SHA256 on raw byte string
  """
  return hashlib.sha256(hashlib.sha256(bytebuffer).digest()).digest()

if __name__ == "__main__":
    pass
    # test_size = 30
    #
    # with open("1M.dat", "rb") as f:
    #     a = f.read(test_size)
    #     print(type(a))
    #     print(a)
    #     print("f.tell(): {}".format(f.tell()))
    #
    #
    # with open("test_block.dat", "rb") as f2:
    #     b = my_read(f2, test_size)
    #     print(type(b))
    #     print(b)
    #     print("f2.tell(): {}".format(f2.tell()))

    # expected_pubkey = "4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac"
    # my_pubkey = "4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac"
    # print(expected_pubkey == my_pubkey)


    # test_pubkey = bytes.fromhex("76a9146949cc8b69a0af5b5d9849f2b1a4b2960e91a91688ac")
    # print(rawpk2addr(test_pubkey))

    # with open("blk710793.dat", "rb") as f:
    #     # e = my_read(f, 0)
    #     a = my_read(f, 2)
    #     b = my_read(f, 2)
    #     c = my_read(f, 2)
    #     # a = f.read(4)
    #     # b = f.read(2)
    #     # c = bytes.fromhex(str(f.read(2).decode("utf-8")))
    #     # print(e)
    #     print(a)
    #     print(b)
    #     print(c)
    #     # print(c == b'\x20')
    #     # print(c == b' ')
    #     d = b''.join([a, b, c])
    #     print(d)
        #
        # print(d == b'\04\x00\x00\x20')

        # print(my_read(f, 2))

        # print(bool(0))
        # print(bool(1))
        # print(bool(2))



