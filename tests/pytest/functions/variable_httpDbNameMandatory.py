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
import subprocess
import random
import math
import numpy as np
import inspect

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
from requests.auth import HTTPBasicAuth
import requests
import json


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self) -> str:
        selfPath = os.path.dirname(os.path.realpath(__file__))
        global cfgPath

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
            cfgPath = projPath + "/community/sim/dnode1/cfg"
        else:
            projPath = selfPath[:selfPath.find("tests")]
            cfgPath = projPath + "/sim/dnode1/cfg"


        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/debug/build/bin")]
                    break
        return buildPath

    def getCfgDir(self) -> str:
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgDir = self.getBuildPath() + "/community/sim/dnode1/cfg"
            
        else:
            cfgDir = self.getBuildPath() + "/sim/dnode1/cfg"

        return cfgDir

    def getCfgFile(self) -> str:
        return self.getCfgDir()+"/taos.cfg"

    def rest_query(self,sql,db=''):                                       
        host = '127.0.0.1'
        user = 'root'
        password = 'taosdata'
        port =6041
        if db == '':
            url = "http://{}:{}/rest/sql".format(host, port )
        else:
            url = "http://{}:{}/rest/sql/{}".format(host, port, db )
        try:
            r = requests.post(url, 
                data = 'use db' ,
                auth = HTTPBasicAuth('root', 'taosdata'))  
            r = requests.post(url, 
                data = sql,
                auth = HTTPBasicAuth('root', 'taosdata'))         
        except:
            print("REST API Failure (TODO: more info here)")
            raise
        rj = dict(r.json()['data'])
        return rj
    
    def TS834(self):
        tdLog.printNoPrefix("==========TS-782==========")
        tdSql.prepare()
        buildPath = self.getBuildPath()
        cfgfile = cfgPath + "/taos.cfg"

        tdSql.execute("show variables")
        res_com = tdSql.cursor.fetchall()
        rescomlist = np.array(res_com)
        cpms_index = np.where(rescomlist == "httpDbNameMandatory")
        index_value = np.dstack((cpms_index[0])).squeeze()

        tdSql.query("show variables")
        tdSql.checkData(index_value, 1, 0)

        rj = self.rest_query("show variables")
        if 'httpDbNameMandatory' not in rj:
            tdLog.info('has no httpDbNameMandatory shown')
            tdLog.exit(1)
        if rj['httpDbNameMandatory'] != '0':
            tdLog.info('httpDbNameMandatory data:%s == expect:0'%rj['httpDbNameMandatory'])
            tdLog.exit(1)
        tdLog.info("httpDbNameMandatory by restful query data:%s == expect:0" % (rj['httpDbNameMandatory']))

        tdSql.query("show dnodes")
        index = tdSql.getData(0, 0)
        tdLog.info("restart taosd ")
        tdDnodes.stop(index)
        cmd = f"echo 'httpDbNameMandatory 1' >> {cfgfile} "
        try:
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
        except Exception as e:
            raise e

        tdDnodes.start(index)
        tdSql.query("show variables")
        tdSql.checkData(index_value, 1, 1)

        rj = self.rest_query("show variables", 'db')
        if 'httpDbNameMandatory' not in rj:
            tdLog.info('has no httpDbNameMandatory shown')
            tdLog.exit(1)
        if rj['httpDbNameMandatory'] != '1':
            tdLog.info('httpDbNameMandatory data:%s == expect:0'%rj['httpDbNameMandatory'])
            tdLog.exit(1)
        tdLog.info("httpDbNameMandatory by restful query data:%s == expect:1" % (rj['httpDbNameMandatory']))

    def run(self):
        
        #TS-834 https://jira.taosdata.com:18080/browse/TS-834
        self.TS834()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())



