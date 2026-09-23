# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-10-21 Alex Duan Migrated from uncatalog/army/query/window/test_window_base.py
    
        """
        self.do_td31660()
