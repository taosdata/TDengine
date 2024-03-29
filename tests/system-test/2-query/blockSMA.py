from wsgiref.headers import tspecials
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10000
        self.ts = 1537146000000

    def run(self):
        dbname = "db"
        tdSql.prepare(dbname=dbname, drop=True, stt_trigger=1)

        tdSql.execute(f'''create table {dbname}.ntb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.ntb values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                        % (self.ts + i, i % 127 + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i % 255 + 1, i + 1, i + 1, i + 1))


        tdSql.execute('flush database db')

        # test functions using sma result
        tdSql.query(f"select count(col1),min(col1),max(col1),avg(col1),sum(col1),spread(col1),percentile(col1, 0),first(col1),last(col1) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 127)
        tdSql.checkData(0, 3, 63.8449)
        tdSql.checkData(0, 4, 638449)
        tdSql.checkData(0, 5, 126.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 94)

        tdSql.query(f"select count(col2),min(col2),max(col2),avg(col2),sum(col2),spread(col2),percentile(col2, 0),first(col2),last(col2) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col3),min(col3),max(col3),avg(col3),sum(col3),spread(col3),percentile(col3, 0),first(col3),last(col3) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col4),min(col4),max(col4),avg(col4),sum(col4),spread(col4),percentile(col4, 0),first(col4),last(col4) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col5),min(col5),max(col5),avg(col5),sum(col5),spread(col5),percentile(col5, 0),first(col5),last(col5) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 0.1)
        tdSql.checkData(0, 2, 9999.09961)
        tdSql.checkData(0, 3, 4999.599985846)
        tdSql.checkData(0, 4, 49995999.858455874)
        tdSql.checkData(0, 5, 9998.999609374)
        tdSql.checkData(0, 6, 0.100000001)
        tdSql.checkData(0, 7, 0.1)
        tdSql.checkData(0, 8, 9999.09961)

        tdSql.query(f"select count(col6),min(col6),max(col6),avg(col6),sum(col6),spread(col6),percentile(col6, 0),first(col6),last(col6) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 0.1)
        tdSql.checkData(0, 2, 9999.100000000)
        tdSql.checkData(0, 3, 4999.600000001)
        tdSql.checkData(0, 4, 49996000.000005305)
        tdSql.checkData(0, 5, 9999.000000000)
        tdSql.checkData(0, 6, 0.1)
        tdSql.checkData(0, 7, 0.1)
        tdSql.checkData(0, 8, 9999.1)

        tdSql.query(f"select count(col11),min(col11),max(col11),avg(col11),sum(col11),spread(col11),percentile(col11, 0),first(col11),last(col11) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 255)
        tdSql.checkData(0, 3, 127.45)
        tdSql.checkData(0, 4, 1274500)
        tdSql.checkData(0, 5, 254.000000000)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 55)

        tdSql.query(f"select count(col12),min(col12),max(col12),avg(col12),sum(col12),spread(col12),percentile(col12, 0),first(col12),last(col12) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col13),min(col13),max(col13),avg(col13),sum(col13),spread(col13),percentile(col13, 0),first(col13),last(col13) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col14),min(col14),max(col14),avg(col14),sum(col14),spread(col14),percentile(col14, 0),first(col14),last(col14) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
