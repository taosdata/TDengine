import time
import uuid
import os
import datetime
from threading import Lock

from taos.cinterface import IS_V3
from taos.constants import FieldType
from taos.precision import PrecisionEnum, PrecisionError
from taos import log
from taos.error import *


tUUIDHashId = 0
tUUIDSerialNo = 0


def gen_req_id():
    global tUUIDHashId
    global tUUIDSerialNo
    if tUUIDHashId == 0:
        uid = str(uuid.uuid4()).encode("utf-8")
        tUUIDHashId = murmurhash3_32(uid, len(uid))

    while True:
        ts = int(time.time()) >> 8
        pid = get_pid()
        with Lock():
            tUUIDSerialNo += 1
            val = tUUIDSerialNo

        req_id = ((tUUIDHashId & 0x07FF) << 52) | ((pid & 0x0F) << 48) | ((ts & 0x3FFFFFF) << 20) | (val & 0xFFFFF)
        if req_id:
            break

    return req_id


def murmurhash3_32(key, length):
    data = key
    nblocks = length >> 2
    h1 = 0x12345678
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    blocks = data
    for i in range(-nblocks, 0):
        k1 = blocks[i]
        k1 *= c1
        k1 = rotl32(k1, 15)
        k1 *= c2
        h1 ^= k1
        h1 = rotl32(h1, 13)
        h1 = h1 * 5 + 0xE6546B64
    tail = data[nblocks * 4 :]
    k1 = 0
    # print('length', length, length & 3)
    if length & 3 == 3:
        k1 ^= tail[2] << 16
    if length & 3 == 2:
        k1 ^= tail[1] << 8
    if length & 3 == 1:
        k1 ^= tail[0]
        k1 *= c1
        k1 = rotl32(k1, 15)
        k1 *= c2
        h1 ^= k1
    h1 ^= length
    fmix32(h1)
    return h1


def rotl32(x, r):
    return (x << r) | (x >> (32 - r))


def fmix32(h):
    h ^= h >> 16
    h *= 0x85EBCA6B
    h ^= h >> 13
    h *= 0xC2B2AE35
    h ^= h >> 16
    return h


def get_pid():
    return os.getpid()


#
# detect list contian none count
#
ALL_NONE = 0  # all list item is none
HAVE_NONE = 1  # list item some is none, some is not none
NOFOUND_NONE = 2  # all list item no found none


def detectListNone(items):
    if items is None:
        return ALL_NONE
    # loop
    n0 = 0  # none count
    n1 = 0  # not noen count
    for item in items:
        if item is None:
            n0 += 1
        else:
            n1 += 1

    # return
    if n0 == 0:
        return NOFOUND_NONE
    if n1 == 0:
        return ALL_NONE
    return HAVE_NONE


def checkString(label, values, types):
    for value in values:
        if value is None:
            continue
        # type
        if type(value) not in types:
            err = f"{label} type bind not support type = {type(value)}"
            raise DataTypeAndRangeError(err)
        # check length should do for engine
        # if length is not None and len(value) + 2 > length:
        #    err = f"{label} type value:{value} length exceeds the max length {length}"
        #    raise DataTypeAndRangeError(err)


def checkNumber(label, values, types, min, max):
    for value in values:
        if value is None:
            continue
        # type
        if type(value) not in types:
            err = f"{label} type bind not support type: {type(value)}"
            raise DataTypeAndRangeError(err)
        # range
        if value < min or value > max:
            err = f"{label} type value:{value} exceeds the indicated range [{min}, {max}]"
            raise DataTypeAndRangeError(err)


def checkTypeValid(buffer_type, values):
    if buffer_type == FieldType.C_TIMESTAMP:
        for value in values:
            if value is None:
                continue
            # check type
            if type(value) not in (int, str, datetime.datetime):
                err = f"timestamp type bind not support type = {type(value)}"
                raise DataTypeAndRangeError(err)
            elif type(value) is int:
                # check range (same bigint)
                min = -(2**63)
                max = 2**63 - 1
                if value < min or value > max:
                    err = f"timestamp type value:{value} exceeds the indicated range [{min}, {max}]"
                    raise DataTypeAndRangeError(err)
    elif buffer_type == FieldType.C_BOOL:
        for value in values:
            if value is None:
                continue
            # check type
            if type(value) not in (bool, int, float):
                err = f"bool type bind not support type = {type(value)}"
                raise DataTypeAndRangeError(err)
    elif buffer_type == FieldType.C_TINYINT:
        checkNumber("tinyint", values, [int, float], -128, 127)
    elif buffer_type == FieldType.C_SMALLINT:
        checkNumber("smallint", values, [int, float], -32768, 32767)
    elif buffer_type == FieldType.C_INT:
        checkNumber("int", values, [int, float], -(2**31), 2**31 - 1)
    elif buffer_type == FieldType.C_BIGINT:
        checkNumber("bigint", values, [int, float], -(2**63), 2**63 - 1)
    elif buffer_type == FieldType.C_FLOAT:
        checkNumber("float", values, [int, float], -3.4e38, 3.4e38)
    elif buffer_type == FieldType.C_DOUBLE:
        checkNumber("double", values, [int, float], -1.7e308, 1.7e308)
    # unsigned
    elif buffer_type == FieldType.C_TINYINT_UNSIGNED:
        checkNumber("unsigned tinyint", values, [int, float], 0, 255)
    elif buffer_type == FieldType.C_SMALLINT_UNSIGNED:
        checkNumber("unsigned smallint", values, [int, float], 0, 65535)
    elif buffer_type == FieldType.C_INT_UNSIGNED:
        checkNumber("unsigned int", values, [int, float], 0, 2**32 - 1)
    elif buffer_type == FieldType.C_BIGINT_UNSIGNED:
        checkNumber("unsigned bigint", values, [int, float], 0, 2**64 - 1)
    # str
    elif buffer_type == FieldType.C_VARCHAR:
        checkString("varchar", values, [str])
    elif buffer_type == FieldType.C_BINARY:
        checkString("binary", values, [str])
    elif buffer_type == FieldType.C_NCHAR:
        checkString("nchar", values, [str])
    elif buffer_type == FieldType.C_JSON:
        checkString("json", values, [str])
    elif buffer_type == FieldType.C_VARBINARY:
        checkString("varbinary", values, [bytes, bytearray])
    elif buffer_type == FieldType.C_GEOMETRY:
        checkString("geometry", values, [bytes, bytearray])
    elif buffer_type == FieldType.C_BLOB:
        checkString("blob", values, [bytes, bytearray])
    else:
        err = f"invalid datatype type={buffer_type} values= {values}"
        raise DataTypeAndRangeError(err)
