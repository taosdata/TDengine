# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

import os
from new_test_framework.utils import tdLog, tdSql

class TestJoinCond:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def check_join(self):
        sql_file = './2-query/join_cond.sql'

        os.system(f'taos -f {sql_file}')
        tdSql.query('use csv_import')
        tdSql.query('SELECT * FROM trans,candle WHERE trans.tradeTime BETWEEN to_timestamp("2010-01-01", "yyyy-mm-dd") AND to_timestamp("2010-01-21", "yyyy-mm-dd")+1d-1a AND candle.date BETWEEN to_timestamp("2010-01-01", "yyyy-mm-dd") AND to_timestamp("2010-01-21", "yyyy-mm-dd") AND timetruncate(trans.tradeTime, 1d) = candle.date AND trans.securityId = cast(substring_index(candle.order_book_id, ".", 1) as INT) AND trans.securityid = 600884;', queryTimes=1)
        tdSql.checkRows(460)
        tdSql.query('SELECT * FROM (SELECT * FROM trans WHERE tradeTime BETWEEN to_timestamp("2010-01-01", "yyyy-mm-dd") AND to_timestamp("2010-01-21", "yyyy-mm-dd")+1d-1a AND securityid = 600884) as t INNER JOIN(SELECT * FROM candle WHERE date BETWEEN to_timestamp("2010-01-01", "yyyy-mm-dd") AND to_timestamp("2010-01-21", "yyyy-mm-dd") ) as c ON timetruncate(t.tradeTime, 1d) = c.date AND t.securityId = cast(substring_index(c.order_book_id, ".", 1) as INT);', queryTimes=1)
        tdSql.checkRows(460)

    def test_join_cond(self):
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

        self.check_join()
        #tdSql.close()

