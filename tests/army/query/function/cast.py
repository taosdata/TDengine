# -*- coding: utf-8 -*-

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.dbname = "cast_db"
        self._datetime_epoch = datetime.datetime.fromtimestamp(0)

    def cast_without_from(self):
        # int
        int_num = 2147483648
        tdSql.query(f"select cast({int_num} as int) re;")
        tdSql.checkData(0, 0, -int_num)

        tdSql.query(f"select cast(2147483647 as int) re;")
        tdSql.checkData(0, 0, 2147483647)

        tdSql.query(f"select cast({int_num} as int unsigned) re;")
        tdSql.checkData(0, 0, int_num)

        tdSql.query(f"select cast({int_num} as bigint) re;")
        tdSql.checkData(0, 0, int_num)

        tdSql.query(f"select cast({int_num} as bigint unsigned) re;")
        tdSql.checkData(0, 0, int_num)

        tdSql.query(f"select cast({int_num} as smallint) re;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast({int_num} as smallint unsigned) re;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast({int_num} as tinyint) re;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast({int_num} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast({int_num} as float) re;")
        tdSql.checkData(0, 0, '2147483648.0')

        tdSql.query(f"select cast({int_num} as double) re;")
        tdSql.checkData(0, 0, '2147483648.0')

        tdSql.query(f"select cast({int_num} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({int_num} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(int_num) / 1000))

        tdSql.query(f"select cast({int_num} as varchar(10)) as re;")
        tdSql.checkData(0, 0, int_num)

        tdSql.query(f"select cast({int_num} as binary(10)) as re;")
        tdSql.checkData(0, 0, int_num)

        sql = f"select cast({int_num} as nchar(10));"
        tdSql.query(sql)
        tdSql.checkData(0, 0, int_num)

        # float
        float_1001 = 3.14159265358979323846264338327950288419716939937510582097494459230781640628620899862803482534211706798214808651328230664709384460955058223172535940812848111745028410270193852110555964462294895493038196442881097566593344612847564823378678316527120190914564856692346034861045432664821339360726024914127372458700660631558817488152092096282925409171536436789259036001133053054882046652138414695194151160943305727036575959195309218611738193261179310511854807446237996274956735188575272489122793818301194912983367336244065664308602139494639522473719070217986094370277053921717629317675238467481846766940513200056812714526356082778577134275778960917363717872146844090122495343014654958537105079227968925892354201995611212902196086403441815981362977477130996051870721134999999837297804995105973173281609631859502445945534690830264252230825334468503526193118817101000313783875288658753320838142061717766914730359825349042875546873115956286388235378759375195778185778053217122680661300192787661119590921642019

        tdSql.query(f"select cast({float_1001} as int) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as int unsigned) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as tinyint) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as tinyint unsigned) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as smallint) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as smallint unsigned) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as bigint) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as bigint unsigned) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as double) as re;")
        tdSql.checkData(0, 0, 3.141592653589793)

        tdSql.query(f"select cast({float_1001} as float) as re;")
        tdSql.checkData(0, 0, 3.1415927)

        tdSql.query(f"select cast({float_1001} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({float_1001} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(float_1001) / 1000))

        sql = f"select cast({float_1001} as varchar(5)) as re;"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 3.141)

        sql = f"select cast({float_1001} as binary(10)) as re;"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 3.141593)

        tdSql.query(f"select cast({float_1001} as nchar(5));")
        tdSql.checkData(0, 0, 3.141)

        # str
        str_410 = "bcdefghigk" * 41
        big_str = "bcdefghigk" * 6552

        tdSql.query(f"select cast('{str_410}' as binary(3)) as re;")
        tdSql.checkData(0, 0, "bcd")

        tdSql.query(f"select cast('{str_410}' as varchar(2)) as re;")
        tdSql.checkData(0, 0, "bc")

        tdSql.query(f"select cast('{str_410}' as nchar(10));")
        tdSql.checkData(0, 0, "bcdefghigk")

        tdSql.query(f"select cast('北京' as nchar(10));")
        tdSql.checkData(0, 0, "北京")

        tdSql.query(f"select cast('{str_410}' as int) as re;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as int unsigned) as re;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as tinyint) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as tinyint unsigned) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as smallint) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as smallint unsigned) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as bigint) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as bigint unsigned) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as float) as re;")
        tdSql.checkData(0, 0, 0.0000000)

        tdSql.query(f"select cast('{str_410}' as double) as re;")
        tdSql.checkData(0, 0, 0.000000000000000)

        tdSql.query(f"select cast('{str_410}' as bool) as re")
        tdSql.checkData(0, 0, False)

        tdSql.query( f"select cast('{str_410}' as timestamp) as re")
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.000")


    def run(self):
        # self.prepare_data()
        # self.all_test()
        # tdSql.execute(f"flush database {self.dbname}")
        # self.all_test()
        self.cast_without_from()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
