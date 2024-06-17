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
    
    VAR_SHOW_LOG_SCOPE_POSITIVE_CASES =['ALL','QUERY','INSERT','OTHERS','NONE','ALL|Query|INSERT|OTHERS|NONE','QUERY|Insert|OTHERS','INSERT|OThers','QUERY|none']
    VAR_SHOW_LOG_SCOPE_NAGATIVE_CASES =['INVLIDVALUE','ALL|INSERT1','QUERY,Insert,OTHERS','OTHERS','NONE','','NULL','100']

    VAR_SHOW_LOG_THRESHOLD_POSITIVE_CASES =['1','2','10','10086','2147483646','2147483647']
    VAR_SHOW_LOG_THRESHOLD_NAGATIVE_CASES =['INVLIDVALUE','-1','0','01','0.1','2147483648','NONE','NULL','']

    VAR_SHOW_LOG_MAX_LEN_POSITIVE_CASES =['1','2','10','10086','999999','1000000']
    VAR_SHOW_LOG_MAX_LEN_NAGATIVE_CASES =['INVLIDVALUE','-1','0','01','0.1','1000001','NONE','NULL','']

    updatecfgDict = {
        "slowLogThresholdTest": "0",  # special setting only for testing
        "monitor": "1",
        "monitorInterval": "5",
        "monitorFqdn": "localhost"
        # "monitorPort": "6041"
    }


    def init1(self):
        tdLog.info(f"check view like.")

        updatecfgDict = {
            "slowLogScope":"ALL"   
        }

        # start taosadapter
        tAdapter.init("")
        tAdapter.deploy()
        tAdapter.start()

        # start taoskeeper
        taoskeeper = os.path.join(os.getcwd(), 'monitor/taoskeeper')
        # cmd = f"nohup {taoskeeper} -c {self.cfg_path} > /dev/null & "
        cmd = f"nohup {taoskeeper} > /dev/null & "
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        # update taos.cfg and restart dnode
        sc.setTaosCfg(1, updatecfgDict)


    def doQuery(self):
        tdLog.info(f"do query.")

        # __group_key
        sql = f"select count(*) from {self.stb} "
        tdSql.query(sql)
        # column index 1 value same with 2
        allRows = self.insert_rows * self.childtable_count
        tdSql.checkFirstValue(sql, allRows)

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

        self.init1()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
