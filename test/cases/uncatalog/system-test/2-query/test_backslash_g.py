# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql
import taos
import os
class TestBackslashG:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def checksql(self, sql):
        result = os.popen(f"taos -s \"{sql}\" ")
        res = result.read()
        print(res)
        if ("Query OK" in res):
            tdLog.info(f"checkEqual success")
        else :
            tdLog.exit(f"checkEqual error")

    def td_28164(self):
        tdSql.execute("drop database if exists td_28164;")
        tdSql.execute("create database td_28164;")
        tdSql.execute("create table td_28164.test (ts timestamp, name varchar(10));")
        tdSql.execute("insert into td_28164.test values (now(), 'ac\\\\G') (now() + 1s, 'ac\\\\G') (now()+2s, 'ac\\G') (now()+3s, 'acG') (now()+4s, 'acK') ;")

        tdSql.query(f"select * from td_28164.test;")
        tdSql.checkRows(5)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\\\\\G';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\\\G';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\G';")
        tdSql.checkRows(2)

        # tdSql.query(f"select * from td_28164.test where name like 'ac\\\g';")
        # tdSql.checkRows(0)
        #
        # tdSql.query(f"select * from td_28164.test where name like 'ac\\g';")
        # tdSql.checkRows(0)

        self.checksql(f'select * from td_28164.test where name like \'ac\\G\'\G;')
        # tdSql.checkRows(2)

        self.checksql(f"select * from td_28164.test where name like \'ac\\G\'   \G;")
        # tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac/\\G';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from td_28164.test where name like 'ac/G';")
        tdSql.checkRows(0)

    def test_backslash_g(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: common,ci,integration,functional
        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        # tdSql.prepare()
        self.td_28164()

        #tdSql.close()

