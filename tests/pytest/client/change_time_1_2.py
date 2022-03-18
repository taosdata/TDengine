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

import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from util.pathFinding import *
from datetime import datetime
import subprocess

##TODO: this is now automatic, but not sure if this will run through jenkins
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdFindPath.init(__file__)

    def run(self):
        tdSql.prepare()
        binPath = tdFindPath.getTaosdemoPath()
        TDenginePath = tdFindPath.getTDenginePath()

        ## change system time to 2020/10/20
        os.system ('timedatectl set-ntp off')
        tdLog.sleep(10)
        os.system ('timedatectl set-time 2020-10-20')

        #run taosdemo to insert data. one row per second from 2020/10/11 to 2020/10/20
        #11 data files should be generated
        #vnode at TDinternal/community/sim/dnode1/data/vnode
        try:
            os.system(f"{binPath}taosdemo -f tools/taosdemoAllTest/manual_change_time_1_1_A.json") 
            commandArray = ['ls', '-l', f'{TDenginePath}/sim/dnode1/data/vnode/vnode2/tsdb/data']
            result = subprocess.run(commandArray, stdout=subprocess.PIPE).stdout.decode('utf-8')
        except BaseException:
            os.system('sudo timedatectl set-ntp on')
            tdLog.sleep(10) 

        if result.count('data') != 11:
            os.system('sudo timedatectl set-ntp on')
            tdLog.sleep(10)
            tdLog.exit('wrong number of files')
        else:
            tdLog.debug("data file number correct")


        try:
            tdSql.query('select first(ts) from stb_0') #check the last data in the database
            tdSql.checkData(0,0,datetime(2020,10,11,0,0,0,0))
        except BaseException:
            os.system('sudo timedatectl set-ntp on')
            tdLog.sleep(10) 

        #moves 5 days ahead to 2020/10/25 and restart taosd
        #4 oldest data file should be removed from tsdb/data
        #7 data file should be found 
        #vnode at TDinternal/community/sim/dnode1/data/vnode

        try:
            os.system ('timedatectl set-time 2020-10-25')
            tdDnodes.stop(1)
            tdDnodes.start(1)
            tdSql.query('select first(ts) from stb_0')
            tdSql.checkData(0,0,datetime(2020,10,14,8,0,0,0)) #check the last data in the database
        except BaseException:
            os.system('sudo timedatectl set-ntp on')
            tdLog.sleep(10) 

        os.system('sudo timedatectl set-ntp on')
        tdLog.sleep(10)
        commandArray = ['ls', '-l', f'{TDenginePath}/sim/dnode1/data/vnode/vnode2/tsdb/data']
        result = subprocess.run(commandArray, stdout=subprocess.PIPE).stdout.decode('utf-8')
        print(result.count('data'))
        if result.count('data') != 7:
            tdLog.exit('wrong number of files')
        else:
            tdLog.debug("data file number correct")
        os.system('sudo timedatectl set-ntp on')
        tdLog.sleep(10)

    def stop(self):
        os.system('sudo timedatectl set-ntp on')
        tdSql.close()
        tdLog.success("alter block manual check finish")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
