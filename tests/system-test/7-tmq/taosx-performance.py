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
    tdDnodes1.stopAll()
    updatecfgDict1 = {'debugFlag': 135, 'serverPort': 6030}
    tdDnodes1.init("./dnode1")
    tdDnodes1.deploy(1,updatecfgDict1)
    tdDnodes1.start(1)

    updatecfgDict2 = {'debugFlag': 135, 'serverPort': 7030}
    tdDnodes2.init("./dnode2")
    tdDnodes2.deploy(1,updatecfgDict2)
    tdDnodes2.start(1)

    os.system("taosBenchmark -f taosx-performance.json")

    tdLog.info("Procedures for tdengine deployed in")
