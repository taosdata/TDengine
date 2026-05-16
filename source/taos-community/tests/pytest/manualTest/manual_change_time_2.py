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

# -*- coding: utf-8 -*-
from fabric import Connection
import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from datetime import datetime
import subprocess
import time
import taos
##TODO: this is now automatic, but not sure if this will run through jenkins

#the initial time used for this test is 2020/10/20

#setting local machine's time for later connecting to the server
os.system('sudo timedatectl set-ntp off')
os.system('sudo timedatectl set-time 2020-10-25')

#connect to VM lyq-1, and initalize the environment at lyq-1
conn1 = Connection("{}@{}".format('ubuntu', "192.168.1.125"), connect_kwargs={"password": "{}".format('tbase125!')})
conn1.run("sudo systemctl stop taosd")
conn1.run('ls -l')
conn1.run('sudo timedatectl set-ntp off')
conn1.run('sudo timedatectl set-time 2020-10-20')

with conn1.cd('/data/taos/log'):
    conn1.run('sudo rm -rf *')

with conn1.cd('/data/taos/data'):
    conn1.run('sudo rm -rf *')

#lanuch taosd and start taosdemo  
conn1.run("sudo systemctl start taosd")
time.sleep(5)
with conn1.cd('~/bschang_test'):
    conn1.run('taosdemo -f manual_change_time_1_1_A.json')

#force everything onto disk
conn1.run("sudo systemctl restart taosd")
time.sleep(10)

#change lyq-1 to 2020/10/25 for testing if the server
#will send data that is out of time range
conn1.run('sudo timedatectl set-time 2020-10-25')

#connect to VM lyq-2, initalize the environment at lyq-2, and run taosd 
#on that
conn2 = Connection("{}@{}".format('ubuntu', "192.168.1.126"), connect_kwargs={"password": "{}".format('tbase125!')})
conn2.run('sudo timedatectl set-ntp off')
conn2.run('sudo timedatectl set-time 2020-10-20')
conn2.run("sudo systemctl stop taosd")
with conn2.cd('/data/taos/log'):
    conn2.run('sudo rm -rf *')
with conn2.cd('/data/taos/data'):
    conn2.run('sudo rm -rf *')
conn2.run("sudo systemctl start taosd")

#set replica to 2
connTaos = taos.connect(host = '192.168.1.125', user = 'root', password = 'taosdata', cnfig = '/etc/taos')
c1 = connTaos.cursor()
c1.execute('create dnode \'lyq-2:6030\'')
c1.execute('alter database db replica 2')
c1.close()
connTaos.close()
time.sleep(5)

#force everything onto the disk for lyq-2
#stopping taosd on lyq-1 for future testing
conn2.run("sudo systemctl stop taosd")
conn1.run("sudo systemctl stop taosd")

#reset the time
conn1.run('sudo timedatectl set-ntp on')
conn2.run('sudo timedatectl set-ntp on')
os.system('sudo timedatectl set-ntp on')

#check if the number of file received is 7
#the 4 oldest data files should be dropped
#4 files because of moving 5 days ahead
with conn2.cd('/data/taos/data/vnode/vnode3/tsdb/data'):
    result = conn2.run('ls -l |grep \'data\' |wc -l')
    if result.stdout.strip() != '7':
        tdLog.exit('the file number is wrong')
    else:
        tdLog.success('the file number is the same. test pass')
