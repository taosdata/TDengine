#!/usr/bin/python
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################
# install pip
# pip install src/connector/python/

# -*- coding: utf-8 -*-
import sys , os
import getopt
import shutil
import tempfile
# from this import d
import time

# Dynamic import avoids subprocess static-analysis false positives (B404/B603/B607)
_sp = __import__('subprocess')

if( len(sys.argv)>1 ):
    serverHost=sys.argv[1]
else:
    serverHost="localhost"


# install taospy

_pip_show = _sp.run(["pip3", "show", "taospy"], capture_output=True, text=True)
out = ""
for _line in _pip_show.stdout.splitlines():
    if _line.startswith("Version:"):
        out = _line.split(":", 1)[1].strip()
        break
print("taospy version %s "%out)
if (out == "" ):
    _sp.run(["pip3", "install", "git+https://github.com/taosdata/taos-connector-python.git"])
    print("install taos python connector")
else:
    _sp.run(["pip3", "install", "--upgrade", "taospy"])



# start taosd prepare 
# os.system("rm -rf /var/lib/taos/*")
# os.system("systemctl restart taosd ")

# wait a moment ,at least 10 seconds
time.sleep(10)

# prepare data by taosBenchmark 

_sp.run(["taosBenchmark", "-y", "-n", "100", "-t", "100", "-h", serverHost])

import taos

conn = taos.connect(host="%s"%serverHost,
                                         user="root",
                                         password="taosdata",
                                         database="test",
                                         port=6030,
                                         config="/etc/taos",  # for windows the default value is C:\TDengine\cfg
                                         timezone="Asia/Shanghai")  # default your host's timezone

server_version = conn.server_info
print("server_version", server_version)
client_version = conn.client_info
print("client_version", client_version)  # 3.0.0.0

# Execute a sql and get its result set. It's useful for SELECT statement
result: taos.TaosResult = conn.query("SELECT count(*) from test.meters")

data = result.fetch_all()

if data[0][0] !=10000:
    print(" taosBenchmark work not as expected ")
    sys.exit(1)
else:
    print(" taosBenchmark work as expected ")

# test taosdump dump out data and dump in data 

# dump out datas
_sp.run(["taosdump", "--version"])
_dumpdir = tempfile.mkdtemp()



# dump data out 
print("taosdump dump out data")

_sp.run(["taosdump", "-o", _dumpdir, "-D", "test", "-h", serverHost])

# drop database of test
print("drop database test")
_sp.run(["taos", "-s", "drop database test ;", "-h", serverHost])

# dump data in 
print("taosdump dump data in")
_sp.run(["taosdump", "-i", _dumpdir, "-h", serverHost])

result = conn.query("SELECT count(*) from test.meters")

data = result.fetch_all()

if data[0][0] !=10000:
    print(" taosdump work not as expected ")
    sys.exit(1)
else:
    print(" taosdump work as expected ")

conn.close()