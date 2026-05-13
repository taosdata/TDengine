###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.components import TaosD
from taostest.util.remote import Remote
from copy import deepcopy
import time
from taostest.util.rest import TDRest
class TestMultiProcessRun(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.taosd = TaosD(self._remote)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.endpoint = self.taosd_setting["spec"]["config"]["firstEP"]
                self.vnodeShmSize_list = self.tdCom.boundary_config["vnodeShmSize"]
                self.vnodeShmSize_default = self.tdCom.boundary_config["vnodeShmSize_default"]
                self.mnodeShmSize_list = self.tdCom.boundary_config["mnodeShmSize"]
                self.mnodeShmSize_default = self.tdCom.boundary_config["mnodeShmSize_default"]
                self.shmfile = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode/shmfile"
                self.log_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"]

    def gen_tb_batch_sql(self, batch, thread_num, col_type, data_type, data_length, ts=None):
        """
        batch_sql
        """
        batch_sqls = ""
        for row_num in range(batch):
            base_sql = ""
            if ts is not None:
                base_sql += f'now-{row_num+1}{thread_num}s, '
            if col_type == "col":
                if data_type == "binary":
                    base_sql += f'"{self.tdCom.get_long_name(length=data_length, mode="letters")}"'
                else:
                    pass
            batch_sqls += f'({base_sql}),'
        return batch_sqls[:-1]

    def check_default_shmsize(self):
        """
        self.vnodeShmSize_default = self.tdCom.boundary_config["vnodeShmSize_default"]
        self.mnodeShmSize_default = self.tdCom.boundary_config["mnodeShmSize_default"]
        """
        self._remote.cmd(self.fqdn, [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
        self.taosd.update_cfg('/tmp', self.taosd_setting, {"multiProcess": 1}, self.endpoint, True)
        vnodeShmSize_infile = self._remote.cmd(self.fqdn, ['cat /var/lib/taos/vnode/shmfile | grep shmsize | cut -f2 -d ":"'])
        vnodeShmid_infile = self._remote.cmd(self.fqdn, ['cat /var/lib/taos/vnode/shmfile | grep shmid | cut -f2 -d ":" | cut -f1 -d ","'])
        vnodeShmSize_ipcs = self._remote.cmd(self.fqdn, [f"ipcs -m | awk \'{{print $2,$5}}\' | grep -w {vnodeShmid_infile} | awk \'{{print $2}}\'"])
        self.tdSql.checkEqual(self.vnodeShmSize_default, int(vnodeShmSize_infile))
        self.tdSql.checkEqual(self.vnodeShmSize_default, int(vnodeShmSize_ipcs))
        mnodeShmSize_ipcs = self._remote.cmd(self.fqdn, [f"ipcs -m | awk \'{{print $2,$5}}\' | grep -w {self.mnodeShmSize_default} | grep -vw {vnodeShmid_infile} | wc -l"])
        self.tdSql.checkEqual(int(mnodeShmSize_ipcs) >= 1, True)

    def check_shmsize_delivery(self):
        """
        check mnode/vnode shmfile/ipcs
        """
        for mnodeShmSize in self.mnodeShmSize_list:
            for vnodeShmSize in self.vnodeShmSize_list:
                self.taosd.update_cfg('/tmp', self.taosd_setting, {"mnodeShmSize": mnodeShmSize, "vnodeShmSize": vnodeShmSize}, self.endpoint, True)
                vnodeShmSize_infile = self._remote.cmd(self.fqdn, ['cat /var/lib/taos/vnode/shmfile | grep shmsize | cut -f2 -d ":"'])
                vnodeShmid_infile = self._remote.cmd(self.fqdn, ['cat /var/lib/taos/vnode/shmfile | grep shmid | cut -f2 -d ":" | cut -f1 -d ","'])
                vnodeShmSize_ipcs = self._remote.cmd(self.fqdn, [f"ipcs -m | awk \'{{print $2,$5}}\' | grep -w {vnodeShmid_infile} | awk \'{{print $2}}\'"])
                self.tdSql.checkEqual(vnodeShmSize, int(vnodeShmSize_infile))
                self.tdSql.checkEqual(vnodeShmSize, int(vnodeShmSize_ipcs))
                mnodeShmSize_ipcs = self._remote.cmd(self.fqdn, [f"ipcs -m | awk \'{{print $2,$5}}\' | grep -w {mnodeShmSize} | grep -vw {vnodeShmid_infile} | wc -l"])
                self.tdSql.checkEqual(int(mnodeShmSize_ipcs) >= 1, True)
        self.log_generation()

    def boundary_check(self):
        """
        self.vnodeShmSize_list = self.tdCom.boundary_config["vnodeShmSize"]
        self.mnodeShmSize_list = self.tdCom.boundary_config["mnodeShmSize"]
        """
        vnodeShmSize_exceeded_list = [min(self.vnodeShmSize_list)-1, max(self.vnodeShmSize_list)+1]
        mnodeShmSize_exceeded_list = [min(self.mnodeShmSize_list)-1, max(self.mnodeShmSize_list)+1]
        for mnodeShmSize in mnodeShmSize_exceeded_list:
            tmp_setting = deepcopy(self.taosd_setting)
            vnodeShmSize = self.vnodeShmSize_default
            self.taosd.update_cfg('/tmp', tmp_setting, {"mnodeShmSize": mnodeShmSize, "vnodeShmSize": vnodeShmSize}, self.endpoint, True)
            taosd_process_count = self._remote.cmd(self.fqdn, [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
            self.tdSql.checkEqual(int(taosd_process_count), 0)
        for vnodeShmSize in vnodeShmSize_exceeded_list:
            tmp_setting = deepcopy(self.taosd_setting)
            mnodeShmSize = self.mnodeShmSize_default
            self.taosd.update_cfg('/tmp', tmp_setting, {"mnodeShmSize": vnodeShmSize, "vnodeShmSize": mnodeShmSize}, self.endpoint, True)
            taosd_process_count = self._remote.cmd(self.fqdn, [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
            self.tdSql.checkEqual(int(taosd_process_count), 0)

    def log_generation(self):
        vnode_log_count = self._remote.cmd(self.fqdn, [f'ls {self.log_dir} | grep vnodelog | wc -l'])
        mnode_log_count = self._remote.cmd(self.fqdn, [f'ls {self.log_dir} | grep mnodelog | wc -l'])
        self.tdSql.checkEqual(int(vnode_log_count) >= 1, True)
        self.tdSql.checkEqual(int(mnode_log_count) >= 1, True)

    def process_count_check(self, check_count=1):
        for process in ["taosd", "taosv", "taosm"]:
            ready_flag = 0
            process_count = self._remote.cmd(self.fqdn, [f'ps -ef | grep {process} | grep -v grep | grep -v SCREEN | grep -v defunct | wc -l'])
            while int(process_count) != check_count:
                process_count = self._remote.cmd(self.fqdn, [f'ps -ef | grep {process} | grep -v grep | grep -v SCREEN | grep -v defunct | wc -l'])
                if ready_flag < 3:
                    ready_flag += 0.5
                    time.sleep(0.5)
                else:
                    return
            self.tdSql.checkEqual(int(process_count), check_count)

    def multi_process_batch_insert(self, batch, col_type="col", data_type="binary", data_length=10000):
        self.taosd.update_cfg('/tmp', self.taosd_setting, {"multiProcess": 1}, self.endpoint, True)
        self.tdCom.drop_all_db()
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create table if not exists {dbname}.tb (ts timestamp, c1 binary({data_length}))')
        self.tdRest.request(f'insert into {dbname}.tb values {self.gen_tb_batch_sql(batch, 1, col_type, data_type, data_length, True)};')
        self.tdRest.request(f'select * from {dbname}.tb')
        #TODO
        self.tdSql.checkEqual(self.tdSql.query_row, batch)
        self.tdRest.request(f'drop database if exists {dbname}')

    def multi_process_threads_batch_insert(self, threads_count, batch, col_type="col", data_type="binary", data_length=10000):
        """
        multi threads batch insert with multi_process mode
        """
        self.taosd.update_cfg('/tmp', self.taosd_setting, {"mnodeShmSize": min(self.mnodeShmSize_list), "vnodeShmSize": min(self.vnodeShmSize_list)}, self.endpoint, True)
        self.tdSql.drop_all_db()
        sql_list = list()
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create table if not exists {dbname}.tb (ts timestamp, c1 binary({data_length}))')
        for thread_num in range(threads_count):
            sql = f'insert into {dbname}.tb values {self.gen_tb_batch_sql(batch, thread_num, col_type, data_type, data_length, True)};'
            sql_list.append(sql)
        tlist = self.tdSql.genMultiThreadSeq(sql_list)
        self.tdSql.multiThreadRun(tlist)
        self.tdRest.request(f'select * from {dbname}.tb')
        #TODO
        self.tdSql.checkEqual(self.tdSql.query_row, batch*threads_count)
        self.tdRest.request(f'drop database if exists {dbname}')

    def kill_auto_restore(self):
        self._remote.cmd(self.fqdn, [f'ps -ef | grep taosm | grep -v grep | awk \'{{print $2}}\' | xargs kill -9 2&>1'])
        self._remote.cmd(self.fqdn, [f'ps -ef | grep taosv | grep -v grep | awk \'{{print $2}}\' | xargs kill -9 2&>1'])
        self.process_count_check()
        self._remote.cmd(self.fqdn, [f'ps -ef | grep taosd | grep -v grep | awk \'{{print $2}}\' | xargs kill -9 2&>1'])
        self.process_count_check(0)
        self.taosd.update_cfg('/tmp', self.taosd_setting, {"multiProcess": 0}, self.endpoint, True)

    def run(self):
        self.check_default_shmsize()
        self.check_shmsize_delivery()
        self.boundary_check()
        self.log_generation()
        self.multi_process_batch_insert(batch=100, data_length=10160)
        self.multi_process_threads_batch_insert(threads_count=6, batch=100, data_length=10160)
        self.kill_auto_restore()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            check_default_shmsize <jayden>: [TD-15391] : check_default_shmsize;\n
            check_shmsize_delivery <jayden>: [TD-15391] : shmsize delivery;\n
            boundary_check <jayden>: [TD-15391] : boundary check;\n
            multi_process_batch_insert <jayden>: [TD-15391] : multi process batch insert(single thread);\n
            multi_process_threads_batch_insert <jayden>: [TD-15391] : multi process batch insert(multi threads);\n
            kill_auto_restore <jayden>: [TD-15391] : kill auto restore;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BatchInsert