from os import system
from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(),True)

    def test_join(self):
        sql_file = './2-query/join_cond.sql'

        os.system(f'taos -f {sql_file}')
        tdSql.query('use csv_import')
        tdSql.query('SELECT * FROM trans,candle WHERE trans.tradeTime BETWEEN to_timestamp("2010-01-01", "yyyy-mm-dd") AND to_timestamp("2010-01-21", "yyyy-mm-dd")+1d-1a AND candle.date BETWEEN to_timestamp("2010-01-01", "yyyy-mm-dd") AND to_timestamp("2010-01-21", "yyyy-mm-dd") AND timetruncate(trans.tradeTime, 1d) = candle.date AND trans.securityId = cast(substring_index(candle.order_book_id, ".", 1) as INT) AND trans.securityid = 600884;', queryTimes=1)
        tdSql.checkRows(460)
        tdSql.query('SELECT * FROM (SELECT * FROM trans WHERE tradeTime BETWEEN to_timestamp("2010-01-01", "yyyy-mm-dd") AND to_timestamp("2010-01-21", "yyyy-mm-dd")+1d-1a AND securityid = 600884) as t INNER JOIN(SELECT * FROM candle WHERE date BETWEEN to_timestamp("2010-01-01", "yyyy-mm-dd") AND to_timestamp("2010-01-21", "yyyy-mm-dd") ) as c ON timetruncate(t.tradeTime, 1d) = c.date AND t.securityId = cast(substring_index(c.order_book_id, ".", 1) as INT);', queryTimes=1)
        tdSql.checkRows(460)

    def run(self):
        self.test_join()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
