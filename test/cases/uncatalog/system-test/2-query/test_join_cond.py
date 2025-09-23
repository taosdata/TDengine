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

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        self.check_join()
        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
