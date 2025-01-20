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
        self._datetime_epoch = datetime.datetime.fromtimestamp(0)

    def cast_from_int_to_other(self):
        # int
        int_num1 = 2147483647
        int_num2 = 2147483648
        tdSql.query(f"select cast({int_num1} as int) re;")
        tdSql.checkData(0, 0, int_num1)

        tdSql.query(f"select cast({int_num2} as int) re;")
        tdSql.checkData(0, 0, -int_num2)

        tdSql.query(f"select cast({int_num2} as int unsigned) re;")
        tdSql.checkData(0, 0, int_num2)

        tdSql.query(f"select cast({int_num1} as bigint) re;")
        tdSql.checkData(0, 0, int_num1)

        tdSql.query(f"select cast({int_num1} as bigint unsigned) re;")
        tdSql.checkData(0, 0, int_num1)

        tdSql.query(f"select cast({int_num1} as smallint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({int_num1} as smallint unsigned) re;")
        tdSql.checkData(0, 0, 65535)

        tdSql.query(f"select cast({int_num1} as tinyint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({int_num1} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast({int_num2} as float) re;")
        tdSql.checkData(0, 0, "2147483648.0")

        tdSql.query(f"select cast({int_num2} as double) re;")
        tdSql.checkData(0, 0, "2147483648.0")

        tdSql.query(f"select cast({int_num2} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({int_num2} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(int_num2) / 1000))

        tdSql.query(f"select cast({int_num1} as varchar(5)) as re;")
        tdSql.checkData(0, 0, "21474")

        tdSql.query(f"select cast({int_num1} as binary(5)) as re;")
        tdSql.checkData(0, 0, "21474")

        tdSql.query(f"select cast({int_num1} as nchar(5));")
        tdSql.checkData(0, 0, "21474")

    def cast_from_bigint_to_other(self):
        # bigint
        bigint_num = 9223372036854775807
        bigint_num2 = 9223372036854775808
        tdSql.query(f"select cast({bigint_num} as int) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({bigint_num} as int unsigned) re;")
        tdSql.checkData(0, 0, 4294967295)

        tdSql.query(f"select cast({bigint_num} as bigint) re;")
        tdSql.checkData(0, 0, bigint_num)

        tdSql.query(f"select cast({bigint_num2} as bigint) re;")
        tdSql.checkData(0, 0, -bigint_num2)

        tdSql.query(f"select cast({bigint_num2} as bigint unsigned) re;")
        tdSql.checkData(0, 0, bigint_num2)

        tdSql.query(f"select cast({bigint_num} as smallint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({bigint_num} as smallint unsigned) re;")
        tdSql.checkData(0, 0, 65535)

        tdSql.query(f"select cast({bigint_num} as tinyint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({bigint_num} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast({bigint_num} as float) re;")
        tdSql.checkData(0, 0, 9.2233720e18)

        tdSql.query(f"select cast({bigint_num} as double) re;")
        tdSql.checkData(0, 0, 9.2233720e18)

        tdSql.query(f"select cast({bigint_num} as bool) as re;")
        tdSql.checkData(0, 0, True)

        # WARN: datetime overflow dont worry
        tdSql.query(f"select cast({bigint_num} as timestamp) as re;")
        # tdSql.checkData(0, 0, "292278994-08-17 15:12:55.807")

        tdSql.query(f"select cast({bigint_num} as varchar(5)) as re;")
        tdSql.checkData(0, 0, "92233")

        tdSql.query(f"select cast({bigint_num} as binary(5)) as re;")
        tdSql.checkData(0, 0, "92233")

        tdSql.query(f"select cast({bigint_num} as nchar(5));")
        tdSql.checkData(0, 0, "92233")

    def cast_from_smallint_to_other(self):
        smallint_num = 32767
        smallint_num2 = 32768
        tdSql.query(f"select cast({smallint_num} as int) re;")
        tdSql.checkData(0, 0, smallint_num)

        tdSql.query(f"select cast({smallint_num} as int unsigned) re;")
        tdSql.checkData(0, 0, smallint_num)

        tdSql.query(f"select cast({smallint_num} as bigint) re;")
        tdSql.checkData(0, 0, smallint_num)

        tdSql.query(f"select cast({smallint_num2} as bigint unsigned) re;")
        tdSql.checkData(0, 0, smallint_num2)

        tdSql.query(f"select cast({smallint_num} as smallint) re;")
        tdSql.checkData(0, 0, smallint_num)

        tdSql.query(f"select cast({smallint_num2} as smallint) re;")
        tdSql.checkData(0, 0, -smallint_num2)

        tdSql.query(f"select cast({smallint_num2} as smallint unsigned) re;")
        tdSql.checkData(0, 0, smallint_num2)

        tdSql.query(f"select cast({smallint_num} as tinyint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({smallint_num} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast({smallint_num} as float) re;")
        tdSql.checkData(0, 0, "32767.0")

        tdSql.query(f"select cast({smallint_num} as double) re;")
        tdSql.checkData(0, 0, "32767.0")

        tdSql.query(f"select cast({smallint_num} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({smallint_num} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(smallint_num) / 1000))

        tdSql.query(f"select cast({smallint_num} as varchar(3)) as re;")
        tdSql.checkData(0, 0, "327")

        tdSql.query(f"select cast({smallint_num} as binary(3)) as re;")
        tdSql.checkData(0, 0, "327")

        tdSql.query(f"select cast({smallint_num} as nchar(3));")
        tdSql.checkData(0, 0, "327")

    def cast_from_tinyint_to_other(self):
        tinyint_num = 127
        tinyint_num2 = 128
        tdSql.query(f"select cast({tinyint_num} as int) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num} as int unsigned) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num} as bigint) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num2} as bigint unsigned) re;")
        tdSql.checkData(0, 0, tinyint_num2)

        tdSql.query(f"select cast({tinyint_num} as smallint) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num} as smallint unsigned) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num} as tinyint) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num2} as tinyint) re;")
        tdSql.checkData(0, 0, -tinyint_num2)

        tdSql.query(f"select cast({tinyint_num2} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, tinyint_num2)

        tdSql.query(f"select cast({tinyint_num} as float) re;")
        tdSql.checkData(0, 0, "127.0")

        tdSql.query(f"select cast({tinyint_num} as double) re;")
        tdSql.checkData(0, 0, "127.0")

        tdSql.query(f"select cast({tinyint_num} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({tinyint_num} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(tinyint_num) / 1000))

        tdSql.query(f"select cast({tinyint_num} as varchar(2)) as re;")
        tdSql.checkData(0, 0, "12")

        tdSql.query(f"select cast({tinyint_num} as binary(2)) as re;")
        tdSql.checkData(0, 0, "12")

        tdSql.query(f"select cast({tinyint_num} as nchar(2));")
        tdSql.checkData(0, 0, "12")

    def cast_from_float_to_other(self):
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

    def cast_from_str_to_other(self):
        # str
        _str = "bcdefghigk"
        str_410 = _str * 41
        str_401 = _str * 40 + "b"
        big_str = _str * 6552

        tdSql.query(f"select cast('{str_410}' as binary(401)) as re;")
        tdSql.checkData(0, 0, str_401)

        tdSql.query(f"select cast('{str_410}' as varchar(401)) as re;")
        tdSql.checkData(0, 0, str_401)

        tdSql.query(f"select cast('{big_str}' as varchar(420)) as re;")
        tdSql.checkData(0, 0, _str * 42)

        tdSql.query(f"select cast('{str_410}' as nchar(401));")
        tdSql.checkData(0, 0, str_401)

        tdSql.query(f"select cast('北京' as nchar(10));")
        tdSql.checkData(0, 0, "北京")

        # tdSql.query(f"select cast('北京涛思数据有限公司' as nchar(6));")
        # tdSql.checkData(0, 0, "北京涛思数据")

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

        tdSql.query(f"select cast('{str_410}' as timestamp) as re")
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.000")

    def cast_from_bool_to_other(self):
        true_val = True
        false_val = False
        tdSql.query(f"select cast({false_val} as int) re, cast({true_val} as int) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as int unsigned) re, cast({true_val} as int unsigned) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as bigint) re, cast({true_val} as bigint) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select cast({false_val} as bigint unsigned) re, cast({true_val} as bigint unsigned) re;"
        )
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as smallint) re, cast({true_val} as smallint) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select cast({false_val} as smallint unsigned) re, cast({true_val} as smallint unsigned) re;"
        )
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as tinyint) re, cast({true_val} as tinyint) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select cast({false_val} as tinyint unsigned) re, cast({true_val} as tinyint unsigned) re;"
        )
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as smallint) re, cast({true_val} as smallint) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as float) re, cast({true_val} as float) re;")
        tdSql.checkData(0, 0, 0.0000000)
        tdSql.checkData(0, 1, 1.0000000)

        tdSql.query(f"select cast({false_val} as double) re, cast({true_val} as double) re;")
        tdSql.checkData(0, 0, 0.000000000000000)
        tdSql.checkData(0, 1, 1.000000000000000)

        tdSql.query(f"select cast({false_val} as bool) re, cast({true_val} as bool) re;")
        tdSql.checkData(0, 0, false_val)
        tdSql.checkData(0, 1, true_val)

        tdSql.query(f"select cast({false_val} as timestamp) re, cast({true_val} as timestamp) re;")
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.000")
        tdSql.checkData(0, 1, "1970-01-01 08:00:00.001")

        tdSql.query(f"select cast({false_val} as varchar(3)) re, cast({true_val} as varchar(3)) re;")
        tdSql.checkData(0, 0, "fal")
        tdSql.checkData(0, 1, "tru")

        tdSql.query(f"select cast({false_val} as binary(3)) re, cast({true_val} as binary(3)) re;")
        tdSql.checkData(0, 0, "fal")
        tdSql.checkData(0, 1, "tru")

        tdSql.query(f"select cast({false_val} as nchar(3)) re, cast({true_val} as nchar(3)) re;")
        tdSql.checkData(0, 0, "fal")
        tdSql.checkData(0, 1, "tru")

    def cast_from_timestamp_to_other(self):
        # ts = self._datetime_epoch
        # tdSql.query(f"select cast({ts} as int) re;")
        # tdSql.checkData(0, 0, None)
        # todo
        pass

    def cast_from_null_to_other(self):
        tdSql.query(f"select cast(null as int) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as int unsigned) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as bigint) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as bigint unsigned) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as smallint) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as smallint unsigned) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as tinyint) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as tinyint unsigned) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as float) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as double) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as bool) as re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as timestamp) as re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as varchar(55)) as re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as binary(5)) as re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as nchar(5));")
        tdSql.checkData(0, 0, None)

    def cast_from_compute_to_other(self):
        add1 = 123
        add2 = 456
        re = 579
        tdSql.query(f"select cast({add1}+{add2} as int) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as int unsigned) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as bigint) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as bigint unsigned) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as smallint) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as smallint unsigned) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as tinyint) re;")
        tdSql.checkData(0, 0, 67)

        tdSql.query(f"select cast({add1}+{add2} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 67)

        tdSql.query(f"select cast({add1}+{add2} as float) re;")
        tdSql.checkData(0, 0, "579.0")

        tdSql.query(f"select cast({add1}+{add2} as double) re;")
        tdSql.checkData(0, 0, "579.0")

        tdSql.query(f"select cast({add1}+{add2} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({add1}+{add2} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(re) / 1000))

        tdSql.query(f"select cast({add1}+{add2} as varchar(2)) as re;")
        tdSql.checkData(0, 0, "57")

        tdSql.query(f"select cast({add1}+{add2} as binary(2)) as re;")
        tdSql.checkData(0, 0, "57")

        tdSql.query(f"select cast({add1}+{add2} as nchar(2));")
        tdSql.checkData(0, 0, "57")

        test_str = "'!@#'"
        tdSql.query(f"select cast({add1}+{test_str} as int) re;")
        tdSql.checkData(0, 0, add1)

        tdSql.query(f"select cast({add1}+{test_str} as bigint) re;")
        tdSql.checkData(0, 0, add1)

        tdSql.query(f"select cast({add1}+{test_str} as smallint) re;")
        tdSql.checkData(0, 0, add1)

        tdSql.query(f"select cast({add1}+{test_str} as tinyint) re;")
        tdSql.checkData(0, 0, add1)

        tdSql.query(f"select cast({add1}+{test_str} as float) re;")
        tdSql.checkData(0, 0, "123.0")

        tdSql.query(f"select cast({add1}+{test_str} as double) re;")
        tdSql.checkData(0, 0, "123.0")

        tdSql.query(f"select cast({add1}+{test_str} as bool) re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({add1}+{test_str} as timestamp) re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(add1) / 1000))

        tdSql.query(f"select cast({add1}+{test_str} as varchar(2)) re;")
        tdSql.checkData(0, 0, "12")

        tdSql.query(f"select cast({add1}+{test_str} as binary(2)) re;")
        tdSql.checkData(0, 0, "12")

        tdSql.query(f"select cast({add1}+{test_str} as nchar(2)) re;")
        tdSql.checkData(0, 0, "12")

    def cast_without_from(self):
        self.cast_from_int_to_other()
        self.cast_from_bigint_to_other()
        self.cast_from_smallint_to_other()
        self.cast_from_tinyint_to_other()
        self.cast_from_float_to_other()
        self.cast_from_str_to_other()
        self.cast_from_bool_to_other()
        self.cast_from_timestamp_to_other()
        self.cast_from_compute_to_other()
        # self.cast_from_null_to_other()

    def run(self):
        # 'from table' case see system-test/2-query/cast.py
        self.cast_without_from()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
