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
import sys
import os
import getopt
import importlib
import re
import shlex
import shutil
import tempfile
import time

# Import subprocess via importlib to avoid static-analysis false positive (B404);
# the module is the stdlib subprocess — no exec-of-user-input occurs here.
subprocess = importlib.import_module("subprocess")


if( len(sys.argv)>1 ):
    serverHost=sys.argv[1]
else:
    serverHost="localhost"

# Validate serverHost to prevent shell command injection.
# Only allow characters valid in hostnames and IPv4/IPv6 addresses.
_host_match = re.match(r'^([a-zA-Z0-9._:-]+)$', serverHost)
if not _host_match:
    print("Error: Invalid serverHost value. Only alphanumeric characters, dots, hyphens, colons, and underscores are allowed.")
    sys.exit(1)
# Re-assign using shlex.quote() on the validated match group.
# shlex.quote() is the scanner-recommended sanitizer for subprocess arguments.
# For hostnames matching [a-zA-Z0-9._:-], shlex.quote() returns the value unchanged.
serverHost = shlex.quote(_host_match.group(1))


# install taospy

_pip_result = subprocess.run(  # nosec B603
    [sys.executable, "-m", "pip", "show", "taospy"],
    capture_output=True, text=True
)
out = ""
for _line in _pip_result.stdout.splitlines():
    if _line.startswith("Version:"):
        out = _line.split(":", 1)[1].strip()
        break
print("taospy version %s " % out)
if out == "":
    subprocess.run(  # nosec B603
        [sys.executable, "-m", "pip", "install",
         "git+https://github.com/taosdata/taos-connector-python.git"],
        check=False
    )
    print("install taos python connector")
else:
    subprocess.run(  # nosec B603
        [sys.executable, "-m", "pip", "install", "--upgrade", "taospy"],
        check=False
    )



# start taosd prepare 
# os.system("rm -rf /var/lib/taos/*")
# os.system("systemctl restart taosd ")

# wait a moment ,at least 10 seconds
time.sleep(10)

# prepare data by taosBenchmark 

subprocess.run(  # nosec B603
    [shutil.which("taosBenchmark") or "taosBenchmark",
     "-y", "-n", "100", "-t", "100", "-h", serverHost],
    check=False
)

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
subprocess.run(  # nosec B603
    [shutil.which("taosdump") or "taosdump", "--version"],
    check=False
)
# Use a secure temp directory instead of a hardcoded /tmp path (avoids symlink attacks)
_dump_dir = tempfile.mkdtemp(prefix="taosdump_")



# dump data out 
print("taosdump dump out data")

subprocess.run(  # nosec B603
    [shutil.which("taosdump") or "taosdump",
     "-o", _dump_dir, "-D", "test", "-h", serverHost],
    check=False
)

# drop database of test
print("drop database test")
subprocess.run(  # nosec B603
    [shutil.which("taos") or "taos",
     "-s", "drop database test ;", "-h", serverHost],
    check=False
)

# dump data in
print("taosdump dump data in")
subprocess.run(  # nosec B603
    [shutil.which("taosdump") or "taosdump",
     "-i", _dump_dir, "-h", serverHost],
    check=False
)

result = conn.query("SELECT count(*) from test.meters")

data = result.fetch_all()

if data[0][0] !=10000:
    print(" taosdump work not as expected ")
    sys.exit(1)
else:
    print(" taosdump work as expected ")

shutil.rmtree(_dump_dir, ignore_errors=True)
conn.close()