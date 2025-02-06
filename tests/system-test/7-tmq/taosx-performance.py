import taos
import sys
import time
import socket
import os
import threading

sys.path.append("../../pytest")

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from taos.tmq import *
sys.path.append("./7-tmq")
from tmqCommon import *

tdDnodes1 = TDDnodes()
tdDnodes2 = TDDnodes()

if __name__ == "__main__":
    args = sys.argv[1:]
    
    insertData = False
    if len(sys.argv[1:]) == 1:
        insertData = True

    if not os.path.isdir("taosx-perf"):
        os.system("mkdir taosx-perf")
        # os.system("git clone https://github.com/brendangregg/FlameGraph.git taosx-perf")
    os.chdir("taosx-perf")
    print(os.getcwd())

    tdDnodes1.stopAll()
    updatecfgDict1 = {'debugFlag': 131, 'serverPort': 6030}
    tdDnodes1.init("./dnode1")
    tdDnodes1.deploy(1,updatecfgDict1)
    tdDnodes1.start(1)

    updatecfgDict2 = {'debugFlag': 131, 'serverPort': 7030}
    tdDnodes2.init("./dnode2")
    tdDnodes2.deploy(1,updatecfgDict2)
    tdDnodes2.start(1)

    os.system("taos -c ./dnode1/sim/dnode1/cfg -s \"drop topic if exists test\"")
    os.system("taos -c ./dnode2/sim/dnode1/cfg -s \"drop database if exists test\"")
    os.system("taos -c ./dnode2/sim/dnode1/cfg -s \"create database test vgroups 8\"")
    if insertData :
        os.system("taosBenchmark -f ../taosx-performance.json")

    print("create test in dst")

    print("start to run taosx")
    os.system("flamegraph -o raw.svg -- taosx run -f \"tmq://root:taosdata@localhost:6030/test?group.id=taosx-new-`date +%s`&timeout=50s&experimental.snapshot.enable=false&auto.offset.reset=earliest&prefer=raw\" -t \"taos://root:taosdata@localhost:7030/test\" > /dev/null 2>&1 &")
    # os.system("taosx run -f \"tmq://root:taosdata@localhost:6030/test?group.id=taosx-new-`date +%s`&timeout=50s&experimental.snapshot.enable=false&auto.offset.reset=earliest&prefer=raw\" -t \"taos://root:taosdata@localhost:7030/test\" > /dev/null 2>&1 &")
    # time.sleep(10)

    # print("start to run perf")
    #os.system("perf record -a -g -F 99 -p `pidof taosx` sleep 60")

    #os.system("perf script | ./stackcollapse-perf.pl| ./flamegraph.pl > flame.svg")

    tdLog.info("Procedures for tdengine deployed in")
