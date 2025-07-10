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

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, TDSetSql
import re

class TestQmemCtrl:
    updatecfgDict = {'forceReadConfig':'1','queryUseMemoryPool':'true','minReservedMemorySize':1025, 'singleQueryMaxMemorySize': 0}
    
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()
        cls.perf_param_list = ['apps','connections','consumers','queries','trans']
        cls.dbname = "db"
        cls.vgroups = 4
        cls.stbname = f'`{tdCom.getLongName(5)}`'
        cls.tbname = f'`{tdCom.getLongName(3)}`'
        cls.db_param = {
            "database":f"{cls.dbname}",
            "buffer":100,
            "vgroups":cls.vgroups,
            "stt_trigger":1,
            "tsdb_pagesize":16
        }

    def update_cfg(self, use_mpool = 1, min_rsize = 0, single_msize = 0):
        updatecfgDict = {'queryUseMemoryPool':f'{use_mpool}','minReservedMemorySize': min_rsize, 'singleQueryMaxMemorySize': single_msize}
        tdDnodes.stop(1)
        tdDnodes.deploy(1, updatecfgDict)
        tdDnodes.starttaosd(1)

    def alter_cfg(self, use_mpool = 1, min_rsize = 0, single_msize = 0):
        tdSql.error(f"alter dnode 1 'queryUseMemoryPool' '{use_mpool}'")
        tdSql.error(f"alter dnode 1 'minReservedMemorySize' '{min_rsize}'")
        tdSql.error(f"alter dnode 1 'singleQueryMaxMemorySize' '{single_msize}'")

    def variables_check(self, err_case = 0, use_mpool = 1, min_rsize = 0, single_msize = 0):
        if err_case == 1:
            tdSql.error("show dnode 1 variables like 'queryUseMemoryPool'")
        else:    
            tdSql.query("show dnode 1 variables like 'queryUseMemoryPool'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 'queryUseMemoryPool')
            tdSql.checkData(0, 2, use_mpool)

        if err_case == 1:
            tdSql.error("show dnode 1 variables like 'minReservedMemorySize'")
        else:    
            tdSql.query("show dnode 1 variables like 'minReservedMemorySize'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 'minReservedMemorySize')
            tdSql.checkData(0, 2, min_rsize)

        if err_case == 1:
            tdSql.error("show dnode 1 variables like 'singleQueryMaxMemorySize'")
        else:    
            tdSql.query("show dnode 1 variables like 'singleQueryMaxMemorySize'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 'singleQueryMaxMemorySize')
            tdSql.checkData(0, 2, single_msize)


    def cfg_check(self):
        tdLog.info(f'[disable pool] start')
        self.update_cfg(0, 1024, 0)
        self.variables_check(0, 0, 1024, 0)

        tdLog.info(f'[enable pool + up limit] start')
        self.update_cfg(1, 1024, 1000000000)
        self.variables_check(0, 1, 1024, 1000000000)

        tdLog.info(f'[enable pool + reserve limit] start')
        self.update_cfg(1, 1000000000, 3000)
        self.variables_check(0, 1, 1000000000, 3000)

        tdLog.info(f'[enable pool + out of reserve] start')
        self.update_cfg(1, 1000000001)
        self.variables_check(1)

        tdLog.info(f'[enable pool + out of single] start')
        self.update_cfg(1, 1000, 1000000001)
        self.variables_check(1)

        tdLog.info(f'[out of pool] start')
        self.update_cfg(2)
        self.variables_check(1)

    def alter_check(self):    
        tdLog.info(f'[alter] start')
        self.update_cfg(1, 1024, 0)
        self.alter_cfg(1, 1024, 0);

    def single_up_check(self):
        tdLog.info(f'[low single] start')
        self.update_cfg(1, 1024, 1)
        tdSql.error("select *, repeat('aaaaaaaaaa',1000) from information_schema.ins_tables")

        tdLog.info(f'[normal single] start')
        self.update_cfg(1, 1024, 100)
        tdSql.query("select *, repeat('aaaaaaaaaa',1000) from information_schema.ins_tables")

    def too_big_reserve(self):
        tdLog.info(f'[too big reserve] start')
        self.update_cfg(1, 1024000)
        
    def test_qmem_ctrl(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        self.cfg_check()
        self.alter_check()
        self.single_up_check()
        self.too_big_reserve()

        tdLog.success("%s successfully executed" % __file__)

