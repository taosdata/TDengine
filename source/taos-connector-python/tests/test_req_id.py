from taos import *


def test_gen_req_id():
    req_id = utils.gen_req_id()
    print(req_id)
    print(hex(req_id))


def test_murmurhash3_32_tail_length_1():
    case1 = "a".encode("utf-8")
    hashed = utils.murmurhash3_32(case1, len(case1))
    print(hashed)


def test_murmurhash3_32_tail_length_2():
    case2 = "python".encode("utf-8")
    hashed = utils.murmurhash3_32(case2, len(case2))
    print(hashed)


def test_murmurhash3_32_tail_length_3():
    case3 = "123".encode("utf-8")
    hashed = utils.murmurhash3_32(case3, len(case3))
    print(hashed)
