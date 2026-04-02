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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom


class TestTsEventBugs:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.dbname = "db"

        # taosBenchmark run
        tdLog.info(f"insert data.")
        jfile = etool.curFile(__file__, "window.json")
        etool.benchMark(json=jfile)
        
    def do_td31660(self):
        # TD-31660
        sql = "select _wstart,_wend,count(voltage),tbname from db.stb partition by tbname event_window start with voltage >2 end with voltage > 15 slimit 5 limit 5"
        tdSql.query(sql)
        tdSql.checkRows(25)
        sql = "select _wstart,_wend,count(voltage),tbname from db.stb partition by tbname count_window(600) slimit 5 limit 5;"
        tdSql.query(sql)
        tdSql.checkRows(25)
        print("do TD-31660 ........................... [passed]")

    # run
    def test_ts_event_bugs(self):
        """Meta bugs
        
        1. Verify bug TD-31660

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-21 Alex Duan Migrated from uncatalog/army/query/window/test_window_base.py
    
        """
        self.do_td31660()
