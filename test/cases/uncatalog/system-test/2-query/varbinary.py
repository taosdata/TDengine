import sys
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.common import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def test(self):
        tdLog.info(" test")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/varbinary_test'%(buildPath)
        print("cmdStr:", cmdStr)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("varbinary_test ret != 0")

        # tdSql.execute(f" create database test")
        # tdSql.execute(f" use test ")
        # tdSql.execute(f" create stable stb (ts timestamp, c1 nchar(32), c2 varbinary(16), c3 float) tags (t1 int, t2 binary(8), t3 varbinary(8))")
        #
        # tdSql.query(f"desc stb")
        # tdSql.checkRows(7)
        # tdSql.checkData(2, 1, 'VARBINARY')
        # tdSql.checkData(2, 2, 16)
        # tdSql.checkData(6, 1, 'VARBINARY')
        # tdSql.checkData(6, 2, 8)

        # tdSql.execute(f" insert into tb1 using stb tags (1, 'tb1_bin1', 'vart1') values (now, 'nchar1', 'varc1', 0.3)")
        # tdSql.execute(f" insert into tb1 values (now + 1s, 'nchar2', null, 0.4)")
        #
        # tdSql.execute(f" insert into tb2 using stb tags (2, 'tb2_bin1', 093) values (now + 2s, 'nchar1', 892, 0.3)")
        # tdSql.execute(f" insert into tb3 using stb tags (3, 'tb3_bin1', 0x7f829) values (now + 3s, 'nchar1', 0x7f829, 0.3)")
        # tdSql.execute(f" insert into tb4 using stb tags (4, 'tb4_bin1', 0b100000010) values (now + 4s, 'nchar1', 0b110000001, 0.3)")
        # tdSql.execute(f" insert into tb4 values (now + 5s, 'nchar1', 0b11000000100000000, 0.3)")
        # tdSql.execute(f" insert into tb5 using stb tags (4, 'tb5_bin1', NULL) values (now + 6s, 'nchar1', 0b10100000011000000110000001, 0.3)")

        # basic query
        # tdSql.query(f"select c2,t3 from stb order by ts")
        # tdSql.checkRows(6)
        # tdSql.checkData(0, 0, '0x7661726331')
        # tdSql.checkData(0, 1, '0x7661727431')
        # tdSql.checkData(1, 0, None)
        # tdSql.checkData(1, 1, '0x7661727431')
        # tdSql.checkData(2, 0, '0x383932')
        # tdSql.checkData(2, 1, '0x303933')
        # tdSql.checkData(3, 0, '0x07f829')
        # tdSql.checkData(3, 1, '0x07f829')
        # tdSql.checkData(4, 0, '0x0181')
        # tdSql.checkData(4, 1, '0x0102')
        # tdSql.checkData(5, 0, '0x02818181')
        # tdSql.checkData(5, 1, None)

        # tdSql.query(f"select ts,c2 from stb order by c2")
        #
        # tdSql.query(f"select c2,t3 from stb where c2 >= 0 order by ts")
        #
        # tdSql.query(f"select c2,t3 from stb where c2 >= 0x0181 order by ts")



    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()
        self.test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
