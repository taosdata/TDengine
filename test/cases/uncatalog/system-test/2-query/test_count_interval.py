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

from new_test_framework.utils import tdLog, tdSql, tdDnodes

class TestCountInterval:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use d")
        
    def test_count_interval(self):
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

        tdSql.execute("drop database if exists d");
        tdSql.execute("create database d");
        tdSql.execute("use d");
        tdSql.execute("create table st(ts timestamp, f int) tags (t int)")
        
        for i in range(-2048, 2047):
            ts = 1626624000000 + i;
            tdSql.execute(f"insert into ct1 using st tags(1) values({ts}, {i})")
            
        tdSql.execute("flush database d")
        for i in range(1638):
            ts = 1648758398208 + i
            tdSql.execute(f"insert into ct1 using st tags(1) values({ts}, {i})")
        tdSql.execute("insert into ct1 using st tags(1) values(1648970742528, 1638)")
        tdSql.execute("flush database d")
        
        tdSql.query("select count(ts) from ct1 interval(17n, 5n)")           
        self.restartTaosd()
        tdSql.query("select count(ts) from ct1 interval(17n, 5n)")           

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

