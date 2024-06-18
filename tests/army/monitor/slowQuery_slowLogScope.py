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
import time
import random

import taos
import frame
import frame.etool
import frame.eutil

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame.server.dnodes import *
from frame.srvCtl import *
from frame.taosadapter import *



class TDTestCase(TBase):
    
    # VAR_SHOW_LOG_SCOPE_POSITIVE_CASES =['ALL','QUERY','INSERT','OTHERS','NONE','ALL|Query|INSERT|OTHERS|NONE','QUERY|Insert|OTHERS','INSERT|OThers','QUERY|none']
    # VAR_SHOW_LOG_SCOPE_NAGATIVE_CASES =['INVLIDVALUE','ALL|INSERT1','QUERY,Insert,OTHERS','OTHERS','NONE','','NULL','100']

    # VAR_SHOW_LOG_THRESHOLD_POSITIVE_CASES =['1','2','10','10086','2147483646','2147483647']
    # VAR_SHOW_LOG_THRESHOLD_NAGATIVE_CASES =['INVLIDVALUE','-1','0','01','0.1','2147483648','NONE','NULL','']

    # VAR_SHOW_LOG_MAX_LEN_POSITIVE_CASES =['1','2','10','10086','999999','1000000']
    # VAR_SHOW_LOG_MAX_LEN_NAGATIVE_CASES =['INVLIDVALUE','-1','0','01','0.1','1000001','NONE','NULL','']


    updatecfgDict = {
        "slowLogThresholdTest": "0",  # special setting only for testing
        "monitor": "1",
        "monitorInterval": "5",
        "monitorFqdn": "localhost"
        # "monitorPort": "6041"
    }


    def initEnv(self):
        tdLog.info(f"check view like.")

        # start taosadapter
        tAdapter.init("")
        tAdapter.deploy()
        # stop taosadapter
        tAdapter.start()

        # start taoskeeper
        self.startTaoskeeper()

        # stop taoskeeper
        self.stopTaoskeeper()

    
    def demo(self):
        updatecfgDict = {
            "slowLogScope":"ALL"   
        }

        # update taos.cfg and restart dnode
        self.updateTaosCfg(1, updatecfgDict)

        # alter dnode's variable
        self.alterVariables(1, updatecfgDict)

        # alter all dnode's variable
        self.alterVariables(updatecfgDict)

    def startTaoskeeper(self):
         # stop taoskeeper
        cmd = 'pkill -f "taoskeeper"'
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def stopTaoskeeper(self):
         # start taoskeeper
        taoskeeper = os.path.join(os.getcwd(), 'monitor/taoskeeper')
        # cmd = f"nohup {taoskeeper} -c {self.cfg_path} > /dev/null & "
        cmd = f"nohup {taoskeeper} > /dev/null & "
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def updateTaosCfg(self, idx, updatecfgDict: list):
        # stop dnode
        sc.dnodeStop(1)

        # update taos.cfg and restart dnode
        sc.setTaosCfg(idx, updatecfgDict)

        # stop dnode
        sc.dnodeStart(1)

    def alterVariables(self, idx: int = 0, updatecfgDict: dict = None):
        if idx == 0:
            for key, value in updatecfgDict.items():
                sql = f"ALTER ALL DNODES  '{key} {value}'"
                tdSql.execute(sql, show=True)
        else:
            for key, value in updatecfgDict.items():
                sql = f"ALTER DNODE {idx} '{key} {value}'"
                tdSql.execute(sql, show=True)
    
    # check default value of slowLogScope
    def checkSlowQueryTable(self, db_name: str, query_log_exist: bool, insert_log_exist: bool, other_log_exist: bool):
        # 
        tdSql.execute(f"create database {db_name}")
        tdSql.execute(f"create table {db_name}.t100 (ts timestamp, pk varchar(20) primary key, c1 varchar(100)) tags (id int)")
        tdSql.execute(f"insert into {db_name}.ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a1', '100')")
        tdSql.execute(f"insert into {db_name}.ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a2', '200')")
        tdSql.query(f"select * from {db_name}.t100 order by ts")

        # check data in taos_slow_sql_detail
        tdSql.query(f"select * from log.taos_slow_sql_detail where db='{db_name}' order by start_ts")
        row_count = 0
        if query_log_exist:
            row_count = row_count + 1
        if insert_log_exist:
            row_count = row_count + 2
        if other_log_exist:
            row_count = row_count + 2
        tdSql.checkRows(row_count)

        tdSql.checkdata(0, 3, 0)
        tdSql.checkdata(0, 4, '')
        tdSql.checkdata(0, 5, 4)
        tdSql.checkdata(0, 6, 0)
        tdSql.checkdata(0, 7, f"create database {db_name}")
        tdSql.checkdata(0, 8, 0)

        # check query records
        tdSql.query(f"select * from log.taos_slow_sql_detail where db='{db_name}' and `type`=1 order by start_ts")
        if query_log_exist:
            tdSql.checkRows(1)
            tdSql.checkdata(0, 3, 0)
            tdSql.checkdata(0, 4, '')
            tdSql.checkdata(0, 5, 4)
            tdSql.checkdata(0, 6, 0)
            tdSql.checkdata(0, 7, f"create database {db_name}")
            tdSql.checkdata(0, 8, 0)
        else:
            tdSql.checkRows(0)
        
        # check insert records
        tdSql.query(f"select * from log.taos_slow_sql_detail where db='{db_name}' and `type`=2 order by start_ts")
        if query_log_exist:
            tdSql.checkRows(1)
            tdSql.checkdata(0, 3, 0)
            tdSql.checkdata(0, 4, '')
            tdSql.checkdata(0, 5, 4)
            tdSql.checkdata(0, 6, 0)
            tdSql.checkdata(0, 7, f"create database {db_name}")
            tdSql.checkdata(0, 8, 0)
        else:
            tdSql.checkRows(0)

        # check others records
        tdSql.query(f"select * from log.taos_slow_sql_detail where db='{db_name}' and `type`=4 order by start_ts")
        if query_log_exist:
            tdSql.checkRows(1)
            tdSql.checkdata(0, 3, 0)
            tdSql.checkdata(0, 4, '')
            tdSql.checkdata(0, 5, 4)
            tdSql.checkdata(0, 6, 0)
            tdSql.checkdata(0, 7, f"create database {db_name}")
            tdSql.checkdata(0, 8, 0)
        else:
            tdSql.checkRows(0)

    def failedToStartTaosdlWithSpecialCfg(self, cfg: str):
        if not os.path.exists(cfg):
            tdLog.exit(f'config file does not exist, file: {cfg}')

        rets = frame.etool.runBinFile("taosd", f" -c {cfg}")
        if frame.etool.checkErrorFromBinFile(rets):
            return True
        else:
            tdLog.exit(f"start taosd with speccial config: {cfg}, can not catch exception when restart taosd")

    # create private taos.cfg by special setting list
    def createPrivateCfg(self, params: dict):
        taos_cfg = os.path.join(os.getcwd(), 'monitor/taos.cfg')
        if os.path.exists(taos_cfg):
            sys.path.remove(taos_cfg)
        
        with open(taos_cfg, 'w') as file:
            for key, value in params:
                file.write(f'{key} {value}')
        file.close()
        return taos_cfg

    def test_show_log_scope(self):
        tdLog.info(f"check_show_log_scope")

        # check nagative value of show_log_scope
        VAR_SHOW_LOG_SCOPE_NAGATIVE_CASES =['INVLIDVALUE','ALL|INSERT1','QUERY,Insert,OTHERS','OTHERS','NONE','','NULL','100']


        # check show_log_scope setting 
        VAR_SHOW_LOG_SCOPE_POSITIVE_CASES = {
            'ALL': {'query_log_exist': 'true', 'insert_log_exist': 'true', 'other_log_exist': 'true'},
            'QUERY': {'query_log_exist': 'true', 'insert_log_exist': 'false', 'other_log_exist': 'false'},
            'INSERT': {'query_log_exist': 'false', 'insert_log_exist': 'true', 'other_log_exist': 'false'},
            'OTHERS': {'query_log_exist': 'false', 'insert_log_exist': 'false', 'other_log_exist': 'true'},
            'NONE': {'query_log_exist': 'false', 'insert_log_exist': 'false', 'other_log_exist': 'false'},
            'ALL|Query|INSERT|OTHERS|NONE': {'query_log_exist': 'true', 'insert_log_exist': 'true', 'other_log_exist': 'true'},
            'QUERY|Insert|OTHERS': {'query_log_exist': 'true', 'insert_log_exist': 'true', 'other_log_exist': 'true'},
            'INSERT|OThers': {'query_log_exist': 'false', 'insert_log_exist': 'true', 'other_log_exist': 'true'},
            'QUERY|none': {'query_log_exist': 'true', 'insert_log_exist': 'false', 'other_log_exist': 'false'}}

        for scope_value, verifications in VAR_SHOW_LOG_SCOPE_POSITIVE_CASES.items():
            updatecfgDict = {"slowLogScope": scope_value}

            # set slowLogScope=ALL via taos.cfg
            self.updateTaosCfg(1, updatecfgDict)
            self.checkSlowQueryTable(db_name='check_default', query_log_exist=verifications['query_log_exist'], insert_log_exist=verifications['insert_log_exist'], other_log_exist=verifications['other_log_exist'])

            # set slowLogScope=ALL via alter operation
            self.alterVariables(1, updatecfgDict)
            self.checkSlowQueryTable(db_name='check_default', query_log_exist=verifications['query_log_exist'], insert_log_exist=verifications['insert_log_exist'], other_log_exist=verifications['other_log_exist'])

        # run sql by taos shell using specified cfg
        params = {"slowLogScope":"ALL"}
        taoscfg = self.createPrivateCfg(params=params)
        eutil.runTaosShellWithSpecialCfg(sql='show dnodes', cfg=taoscfg)

        tdLog.info(f"check_show_log_scope is done")

    def checkShowTags(self):
        # verification for TD-29904
        tdSql.error("show tags from t100000", expectErrInfo='Fail to get table info, error: Table does not exist')

        sql = "show tags from child1"
        tdSql.query(sql)
        tdSql.checkRows(5)

        sql = f"show tags from child1 from {self.db}"
        tdSql.query(sql)
        tdSql.checkRows(5)

        sql = f"show tags from {self.db}.child1"
        tdSql.query(sql)
        tdSql.checkRows(5)

        # verification for TD-30030
        tdSql.execute("create table t100 (ts timestamp, pk varchar(20) primary key, c1 varchar(100)) tags (id int)")
        tdSql.execute("insert into ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a1', '100')")
        tdSql.execute("insert into ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a2', '200')")
        tdSql.execute("insert into ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a3', '300')")
        tdSql.execute("insert into ct2 using t100 tags(2) values('2024-05-17 14:58:52.902', 'a2', '200')")
        tdSql.execute("create view v100 as select * from t100")
        tdSql.execute("create view v200 as select * from ct1")

        tdSql.error("show tags from v100", expectErrInfo="Tags can only applied to super table and child table")
        tdSql.error("show tags from v200", expectErrInfo="Tags can only applied to super table and child table")

        tdSql.execute("create table t200 (ts timestamp, pk varchar(20) primary key, c1 varchar(100))")

        tdSql.error("show tags from t200", expectErrInfo="Tags can only applied to super table and child table")

    def checkShow(self):
        # not support
        sql = "show accounts;"
        tdSql.error(sql)

        # check result
        sql = "SHOW CLUSTER;"
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = "SHOW COMPACTS;"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "SHOW COMPACT 1;"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "SHOW CLUSTER MACHINES;"
        tdSql.query(sql)
        tdSql.checkRows(1)

        # run to check crash 
        sqls = [
            "show scores;",
            "SHOW CLUSTER VARIABLES",
            # "SHOW BNODES;",
        ]
        tdSql.executes(sqls)

        self.checkShowTags()


    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        

        self.initEnv()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
