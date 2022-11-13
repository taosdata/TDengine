import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):
        dbname = "db"
        stb = f"{dbname}.stb1"
        rows = 10

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        tdSql.execute(
            f'''create table if not exists {stb}
            (ts timestamp, c1 int, c2 float, c3 bigint, c4 double, c5 smallint, c6 tinyint)
            tags(location binary(64), type int, isused bool , family nchar(64))'''
        )
        tdSql.execute(f"create table {dbname}.t1 using {stb} tags('beijing', 1, 1, 'nchar1')")
        tdSql.execute(f"create table {dbname}.t2 using {stb} tags('shanghai', 2, 0, 'nchar2')")

        tdLog.printNoPrefix("==========step2:insert data")
        for i in range(rows):
            tdSql.execute(
                f"insert into {dbname}.t1 values (now()+{i}m, {32767+i}, {20.0+i/10}, {2**31+i}, {3.4*10**38+i/10}, {127+i}, {i})"
            )
            tdSql.execute(
                f"insert into {dbname}.t2 values (now()-{i}m, {-32767-i}, {20.0-i/10}, {-i-2**31}, {-i/10-3.4*10**38}, {-127-i}, {-i})"
            )
        tdSql.execute(
            f"insert into {dbname}.t1 values (now()+11m, {2**31-1}, {pow(10,37)*34}, {pow(2,63)-1}, {1.7*10**308}, 32767, 127)"
        )
        tdSql.execute(
            f"insert into {dbname}.t2 values (now()-11m, {1-2**31}, {-3.4*10**38}, {1-2**63}, {-1.7*10**308}, -32767, -127)"
        )
        tdSql.execute(
            f"insert into {dbname}.t2 values (now()-12m, null , {-3.4*10**38}, null , {-1.7*10**308}, null , null)"
        )

        tdLog.printNoPrefix("==========step3:query timestamp type")

        tdSql.query(f"select * from {dbname}.t1 where ts between now()-1m and now()+10m")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where ts between '2021-01-01 00:00:00.000' and '2121-01-01 00:00:00.000'")
        # tdSql.checkRows(11)
        tdSql.query(f"select * from {dbname}.t1 where ts between '1969-01-01 00:00:00.000' and '1969-12-31 23:59:59.999'")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where ts between -2793600 and 31507199")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where ts between 1609430400000 and 4765104000000")
        tdSql.checkRows(rows+1)

        tdLog.printNoPrefix("==========step4:query int type")

        tdSql.query(f"select * from {dbname}.t1 where c1 between 32767 and 32776")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c1 between 32766.9 and 32776.1")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c1 between 32776 and 32767")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c1 between 'a' and 'e'")
        tdSql.checkRows(0)
        # tdSql.query("select * from {dbname}.t1 where c1 between 0x64 and 0x69")
        # tdSql.checkRows(6)
        tdSql.query(f"select * from {dbname}.t1 where c1 not between 100 and 106")
        tdSql.checkRows(rows+1)
        tdSql.query(f"select * from {dbname}.t1 where c1 between {2**31-2} and {2**31+1}")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c1 between null and {1-2**31}")
        # tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.t2 where c1 between {-2**31} and {1-2**31}")
        tdSql.checkRows(1)

        tdLog.printNoPrefix("==========step5:query float type")

        tdSql.query(f"select * from {dbname}.t1 where c2 between 20.0 and 21.0")
        tdSql.checkRows(10)
        tdSql.query(f"select * from {dbname}.t1 where c2 between {-3.4*10**38-1} and {3.4*10**38+1}")
        tdSql.checkRows(rows+1)
        tdSql.query(f"select * from {dbname}.t1 where c2 between 21.0 and 20.0")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c2 between 'DC3' and 'SYN'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c2 not between 0.1 and 0.2")
        tdSql.checkRows(rows+1)
        tdSql.query(f"select * from {dbname}.t1 where c2 between {pow(10,38)*3.4} and {pow(10,38)*3.4+1}")
        # tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c2 between {-3.4*10**38-1} and {-3.4*10**38}")
        # tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.t2 where c2 between null and {-3.4*10**38}")
        # tdSql.checkRows(3)

        tdLog.printNoPrefix("==========step6:query bigint type")

        tdSql.query(f"select * from {dbname}.t1 where c3 between {2**31} and {2**31+10}")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c3 between {-2**63} and {2**63}")
        tdSql.checkRows(rows+1)
        tdSql.query(f"select * from {dbname}.t1 where c3 between {2**31+10} and {2**31}")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c3 between 'a' and 'z'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c3 not between 1 and 2")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c3 between {2**63-2} and {2**63-1}")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c3 between {-2**63} and {1-2**63}")
        # tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.t2 where c3 between null and {1-2**63}")
        # tdSql.checkRows(2)

        tdLog.printNoPrefix("==========step7:query double type")

        tdSql.query(f"select * from {dbname}.t1 where c4 between {3.4*10**38} and {3.4*10**38+10}")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c4 between {1.7*10**308+1} and {1.7*10**308+2}")
        # 因为精度原因，在超出bigint边界后，数值不能进行准确的判断
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c4 between {3.4*10**38+10} and {3.4*10**38}")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c4 between 'a' and 'z'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c4 not between 1 and 2")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c4 between {1.7*10**308} and {1.7*10**308+1}")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c4 between {-1.7*10**308-1} and {-1.7*10**308}")
        # tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.t2 where c4 between null and {-1.7*10**308}")
        # tdSql.checkRows(3)

        tdLog.printNoPrefix("==========step8:query smallint type")

        tdSql.query(f"select * from {dbname}.t1 where c5 between 127 and 136")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c5 between 126.9 and 135.9")
        tdSql.checkRows(rows-1)
        tdSql.query(f"select * from {dbname}.t1 where c5 between 136 and 127")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c5 between '~' and '^'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c5 not between 1 and 2")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c5 between 32767 and 32768")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c5 between -32768 and -32767")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c5 between null and -32767")
        # tdSql.checkRows(1)

        tdLog.printNoPrefix("==========step9:query tinyint type")

        tdSql.query(f"select * from {dbname}.t1 where c6 between 0 and 9")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c6 between -1.1 and 8.9")
        tdSql.checkRows(rows-1)
        tdSql.query(f"select * from {dbname}.t1 where c6 between 9 and 0")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c6 between 'NUL' and 'HT'")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t1 where c6 not between 1 and 2")
        # tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t1 where c6 between 127 and 128")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c6 between -128 and -127")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c6 between null and -127")
        # tdSql.checkRows(3)

        tdLog.printNoPrefix("==========step10:invalid query type")

        # TODO tag is not finished
        tdSql.query(f"select * from {stb} where location between 'beijing' and 'shanghai'")
        tdSql.checkRows(rows * 2 + 3)
        # 非0值均解析为1，因此"between 负值 and o"解析为"between 1 and 0"
        tdSql.query(f"select * from {stb} where isused between 0 and 1")
        tdSql.checkRows(rows * 2 + 3)
        tdSql.query(f"select * from {stb} where isused between -1 and 0")
        tdSql.checkRows(rows + 2)
        tdSql.query(f"select * from {stb} where isused between false and true")
        tdSql.checkRows(rows * 2 + 3)
        tdSql.query(f"select * from {stb} where family between '拖拉机' and '自行车'")
        tdSql.checkRows(0)

        tdLog.printNoPrefix("==========step11:query HEX/OCT/BIN type")

        tdSql.error(f"select * from {dbname}.t1 where c6 between 0x7f and 0x80")      # check filter HEX
        tdSql.error(f"select * from {dbname}.t1 where c6 between 0b1 and 0b11111")    # check filter BIN
        tdSql.error(f"select * from {dbname}.t1 where c6 between 0b1 and 0x80")
        tdSql.error(f"select * from {dbname}.t1 where c6=0b1")
        tdSql.error(f"select * from {dbname}.t1 where c6=0x1")
        # 八进制数据会按照十进制数据进行判定
        tdSql.query(f"select * from {dbname}.t1 where c6 between 01 and 0200")        # check filter OCT
        tdSql.checkRows(rows)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
