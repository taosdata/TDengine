import os
import taos
from multiprocessing import Process
import threading
import sys
sys.path.append("../pytest")
from util.common import tdCom


tag_name = "tag1"    

def add_meta(conn, stb_name, tag_name, tag_type):
    print(tag_name, tag_type)
    print(f"alter stable {stb_name} add tag {tag_name} {tag_type};")
    conn.execute(f"alter stable {stb_name} add tag {tag_name} {tag_type};")

def drop_meta(conn, stb_name, tag_name, tag_type):
    print(tag_name, tag_type)
    print(f"alter stable {stb_name} drop tag {tag_name} ;")
    conn.execute(f"alter stable {stb_name} drop tag {tag_name} ;")

def modify_meta(conn, ctb_name, stb_name, tag_name, tag_type):
    try:
        conn.execute(f"CREATE TABLE {ctb_name} USING {stb_name} ({tag_name}) TAGS (\"tag\");")
    except Exception as e:
        pass 
    print(tag_name, tag_type)
    print(f"ALTER STABLE {stb_name} MODIFY TAG {tag_name} {tag_type};")
    conn.execute(f"ALTER STABLE {stb_name} MODIFY TAG {tag_name} {tag_type};")

def add_drop_meta(conn, ctb_name, stb_name, tag_name, tag_type):
    print(tag_name, tag_type)
    conn.execute(f"alter stable {stb_name} add tag {tag_name} {tag_type};")
    print(f"CREATE TABLE {ctb_name} USING {stb_name} ({tag_name}) TAGS (1);")
    try:
        conn.execute(f"CREATE TABLE {ctb_name} USING {stb_name} ({tag_name}) TAGS (1);")
    except Exception as e:
        pass 
    new_tag_name = f"new_{tag_name}"
    conn.execute(f"ALTER STABLE {stb_name} RENAME TAG {tag_name} {new_tag_name};")
    drop_meta(conn, stb_name, new_tag_name, tag_type)

import random


def generate_random_str(randomlength=32):
    """
    生成一个指定长度的随机字符串
    """
    random_str = ''
    base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz1234567890'
    length = len(base_str) - 1
    for i in range(randomlength):
        random_str += base_str[random.randint(0, length)]
    return random_str
 
str_1000=generate_random_str(1000)
str_1001=generate_random_str(1001)



try:
    conn = taos.connect()
except Exception as e:
    print(str(e))
conn.execute(f"use stream_test")
db_name = "stream_test"

stb_name = f"{db_name}.meters1"
tag_name = "tag_char1"
tag_type = "nchar(4000)"
ctb_name = f"{db_name}.ct_1"
add_meta(conn, stb_name, tag_name, tag_type)
try:
    print(f"CREATE TABLE  {ctb_name} USING {stb_name}  ({tag_name}) TAGS ({str_1000});")
    conn.execute(f"CREATE TABLE  {ctb_name} USING {stb_name}  ({tag_name}) TAGS (\"{str_1000}\");")
except Exception as e:
    pass 

conn.execute(f"insert into {ctb_name} (ts,fc,bin) values(now, 1.2, \"tdengine\");")
for index in range(1,1000000):
    # print(index)
    new_tag_value = generate_random_str(4000)
    # print(f"ALTER TABLE {ctb_name} SET TAG {tag_name}=\"{new_tag_value}\";")
    conn.execute(f"ALTER TABLE {ctb_name} SET TAG {tag_name}=\"{new_tag_value}\";")
    

# tag_type_all = tdCom.full_type_list

# for idx in range(1, 1000):
#     stb_name = f"{db_name}.meters1"
#     ctb_name = f"{db_name}.ct_{idx}"
#     tag_name = "tag" + str(idx)
#     temp_index = idx % len(tag_type_all)
#     tag_type = tag_type_all[temp_index]
#     if tag_type in ["varchar", "binary", "nchar"]:
#         tag_type += f"({idx})"

#     add_drop_meta(conn, ctb_name, stb_name, tag_name, tag_type)
#     tag_name = "tag"




# for idx in range(11, 1000):
#     stb_name = "meters1"
#     ctb_name = f"{db_name}.ct_{idx}"
#     tag_name = "tag" + str(idx)
#     temp_index = idx % len(tag_type_all)
#     tag_type = tag_type_all[temp_index]

#     if tag_type in ["varchar", "binary", "nchar"]:
#         tag_type += f"({idx})"
#         add_meta(conn, stb_name, tag_name, tag_type) 
#         for new_idx in range(1, 100):
#             new_tag_type = tag_type_all[temp_index]
#             new_tag_type += f"({new_idx+idx})"
#             modify_meta(conn, ctb_name, stb_name, tag_name, new_tag_type)
#         drop_meta(conn, stb_name, tag_name, new_tag_type)
#         tag_name = "tag"
