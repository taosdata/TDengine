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
import socket
import taos
import frame
import frame.etool
import frame.eutil
import shutil

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


    def init_env(self):
        tdLog.info(f"check view like.")

        # start taosadapter
        tAdapter.init("")
        tAdapter.deploy()
        # # stop taosadapter
        tAdapter.start()

        # start taoskeeper
        self.start_taoskeeper()

        # stop taoskeeper
        # self.stop_taoskeeper()

    
    def demo(self):
        updatecfgDict = {
            "slowLogScope":"ALL"   
        }

        # update taos.cfg and restart dnode
        self.update_taos_cfg(1, updatecfgDict)

        # alter dnode's variable
        self.alter_variables(1, updatecfgDict)

        # alter all dnode's variable
        self.alter_variables(updatecfgDict)

    def stop_taoskeeper(self):
         # stop taoskeeper
        cmd = 'killall taoskeeper'
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def start_taoskeeper(self):
         # start taoskeeper
        taoskeeper = os.path.join(os.getcwd(), 'monitor/taoskeeper')
        # cmd = f"nohup {taoskeeper} -c {self.cfg_path} > /dev/null & "
        cmd = f"nohup {taoskeeper} > /dev/null & "
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def start_private_taosd(self, cfg: str):
        cmd = f"nohup taosd -c {cfg} > /dev/null & "
        if os.system(cmd) != 0:
            tdLog.exit(cmd)
    
    def stop_private_taosd(self, cfg: str):
        # get process id of taosd
        psCmd = "ps -ef | grep -w taosd | grep 'root' | grep '{0}' | grep -v grep| grep -v defunct | awk '{{print $2}}' | xargs".format(cfg)
        process_id = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
        
        # stop taosd process
        if process_id != '':
            cmd = f"kill -9 {process_id}"
            os.system(cmd)

    def update_taos_cfg(self, idx, updatecfgDict: list):
        # stop dnode
        sc.dnodeStop(idx)

        # update taos.cfg and restart dnode
        sc.setTaosCfg(idx, updatecfgDict)

        # start dnode
        sc.dnodeStart(idx)

    def alter_variables(self, idx: int = 0, updatecfgDict: dict = None):
        if idx == 0:
            for key, value in updatecfgDict.items():
                sql = f"ALTER ALL DNODES  '{key} {value}'"
                tdSql.execute(sql, show=True)
        else:
            for key, value in updatecfgDict.items():
                sql = f"ALTER DNODE {idx} '{key} {value}'"
                tdSql.execute(sql, show=True)
    
    # check default value of slowLogScope
    def check_slow_query_table(self, db_name: str, query_log_exist: bool, insert_log_exist: bool, other_log_exist: bool, taosc_cfg: str = None):
        if taosc_cfg:
            eutil.runTaosShellWithSpecialCfg(sql=f'create database {db_name}', cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"create table {db_name}.t100 (ts timestamp, pk varchar(20) primary key, c1 varchar(100)) tags (id int)", cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"insert into {db_name}.ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a1', '100')", cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"insert into {db_name}.ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a2', '200')", cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"select * from {db_name}.t100 order by ts", cfg=taosc_cfg)
        else:
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

    def failed_to_start_taosd_with_special_cfg(self, cfg: str):
        if not os.path.exists(cfg):
            tdLog.exit(f'config file does not exist, file: {cfg}')

        try:
            rets = frame.etool.runBinFile("taosd", f" -c {cfg}", timeout=3)
            if not frame.etool.checkErrorFromBinFile(rets):
                tdLog.exit(f"start taosd with special config file successfully, no error found, config: {cfg}")
            else:
                catch_err = False
                for value in rets:
                    if 'statusInterval' in str(value).lower():
                        catch_err = True
                        break
                if not catch_err:
                    tdLog.exit(f"start taosd with special config file successfully, no error found, config: {cfg}")
        except subprocess.TimeoutExpired as err_msg:
            tdLog.exit(f"start taosd with special config file successfully, no error found, config: {cfg}")
        

    def failed_to_create_dnode_with_setting_not_match(self, cfg: str, taos_error_dir: str, endpoint: str, err_msg: str):
        if not os.path.exists(cfg):
            tdLog.exit(f'config file does not exist, file: {cfg}')

        # remove taos data folder
        if os.path.exists(taos_error_dir):
            shutil.rmtree(taos_error_dir)

        # stop taosd with special cfg
        self.stop_private_taosd(cfg)

        # drop old dnode if exists
        ret = tdSql.getResult(f"select id from information_schema.ins_dnodes where endpoint='{endpoint}'")

        if len(ret) == 1:
            node_id = ret[0][0]
            tdSql.execute(f"drop dnode '{node_id}'")

        # start taosd with special cfg
        self.start_private_taosd(cfg)
        
        time.sleep(1)

        # create new dnode
        tdSql.execute(f"create dnode '{endpoint}'")

        # check error msg
        count = 0
        while True:
            count = count + 1
            ret = tdSql.getResult(f"select note from information_schema.ins_dnodes where endpoint='{endpoint}'")
            
            if len(ret) == 1:
                note = ret['data'][0][0]
                if err_msg in note:
                    tdLog.info(f"get the expected err_msg: {err_msg}")
                    break
            
            if count == 3:
                tdLog.exit(f"cannot get the expected err_msg: {err_msg}")
            
    # create private taos.cfg by special setting list
    def create_private_cfg(self, cfg_name: str, params: dict):
        taos_cfg = os.path.join(os.getcwd(), f'monitor/{cfg_name}')
        if os.path.exists(taos_cfg):
            os.remove(taos_cfg)
        
        with open(taos_cfg, 'w') as file:
            for key, value in params.items():
                file.write(f'{key} {value}\n')
        file.close()
        return taos_cfg

    def test_show_log_scope(self):
        tdLog.info(f"check_show_log_scope")

        # # 1.check nagative value of show_log_scope
        # VAR_SHOW_LOG_SCOPE_NAGATIVE_CASES =['INVLIDVALUE','ALL|INSERT1','QUERY,Insert,OTHERS','','NULL','100']
        # taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')
        # # endpoint = f'{socket.gethostname()}:6630'
        # for value in VAR_SHOW_LOG_SCOPE_NAGATIVE_CASES:
        #     params = {
        #         'slowLogScope': value,
        #         'fqdn': socket.gethostname(),
        #         'firstEp': f'{socket.gethostname()}:6030',
        #         'serverPort': '6630',
        #         'dataDir': f'{taos_error_dir}/data',
        #         'logDir': f'{taos_error_dir}/log',
        #         'slowLogThresholdTest': '0',
        #         'monitorInterval': '5',
        #         'monitorFqdn': 'localhost'}
        #     new_taos_cfg = self.create_private_cfg(cfg_name='invalid_taos.cfg', params=params)
        #     self.failed_to_start_taosd_with_special_cfg(cfg=new_taos_cfg)
        #     tdLog.info(f"check invalid value '{value}' of 'statusInterval' - PASS")

        # # 2. check valid setting of show_log_scope
        # VAR_SHOW_LOG_SCOPE_POSITIVE_CASES = {
        #     'ALL': {'query_log_exist': 'true', 'insert_log_exist': 'true', 'other_log_exist': 'true'},
        #     'QUERY': {'query_log_exist': 'true', 'insert_log_exist': 'false', 'other_log_exist': 'false'},
        #     'INSERT': {'query_log_exist': 'false', 'insert_log_exist': 'true', 'other_log_exist': 'false'},
        #     'OTHERS': {'query_log_exist': 'false', 'insert_log_exist': 'false', 'other_log_exist': 'true'},
        #     'NONE': {'query_log_exist': 'false', 'insert_log_exist': 'false', 'other_log_exist': 'false'},
        #     'ALL|Query|INSERT|OTHERS|NONE': {'query_log_exist': 'true', 'insert_log_exist': 'true', 'other_log_exist': 'true'},
        #     'QUERY|Insert|OTHERS': {'query_log_exist': 'true', 'insert_log_exist': 'true', 'other_log_exist': 'true'},
        #     'INSERT|OThers': {'query_log_exist': 'false', 'insert_log_exist': 'true', 'other_log_exist': 'true'},
        #     'QUERY|none': {'query_log_exist': 'true', 'insert_log_exist': 'false', 'other_log_exist': 'false'}}

        # for scope_value, verifications in VAR_SHOW_LOG_SCOPE_POSITIVE_CASES.items():
        #     updatecfgDict = {"slowLogScope": scope_value}

        #     # set slowLogScope=ALL via taos.cfg
        #     self.update_taos_cfg(1, updatecfgDict)
        #     self.check_slow_query_table(db_name='check_default', query_log_exist=verifications['query_log_exist'], insert_log_exist=verifications['insert_log_exist'], other_log_exist=verifications['other_log_exist'])

        #     # set slowLogScope=ALL via alter operation
        #     self.alter_variables(1, updatecfgDict)
        #     self.check_slow_query_table(db_name='check_default', query_log_exist=verifications['query_log_exist'], insert_log_exist=verifications['insert_log_exist'], other_log_exist=verifications['other_log_exist'])

        # # 3.config in client is not available
        # updatecfgDict = {"slowLogScope": "INSERT"}
        # self.update_taos_cfg(1, updatecfgDict)

        # updatecfgDict = {"slowLogScope":"QUERY"}
        # taosc_cfg = self.create_private_cfg(cfg_name='taosc.cfg', params=updatecfgDict)
        # self.check_slow_query_table(db_name='check_setting_in_client', taosc_cfg=taosc_cfg, query_log_exist=False, insert_log_exist=True, other_log_exist=False)

        # 4.add node failed if setting is different
        VAR_SHOW_LOG_SCOPE_DIFF_CASES =['ALL','ALL|INSERT','QUERY','OTHERS','NONE']
        taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')
        endpoint = f'{socket.gethostname()}:6630'
        for value in VAR_SHOW_LOG_SCOPE_DIFF_CASES:
            params = {
                'slowLogScope': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'slowLogThresholdTest': '0',
                'monitorInterval': '5',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = self.create_private_cfg(cfg_name='diff_taos.cfg', params=params)
            
            self.failed_to_create_dnode_with_setting_not_match(cfg=new_taos_cfg, taos_error_dir=taos_error_dir, endpoint=endpoint, err_msg='slowLogScope not match')
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

        

        self.init_env()

        self.test_show_log_scope()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
