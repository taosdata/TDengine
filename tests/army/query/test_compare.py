from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *


class TDTestCase(TBase):
    """Add test case to improve the compare.c coverage
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def prepare_data(self):
        # database
        tdSql.execute("create database db;")
        tdSql.execute("use db;")
        
        # create table
        tdSql.execute("create table rt(ts timestamp, c_int8 tinyint, c_uint8  tinyint unsigned, c_bool bool, c_int16 smallint, c_uint16 smallint unsigned, c_int32 int, c_uint32 int unsigned, c_float float, c_int64 bigint, c_uint64 bigint unsigned, c_double double, c_binary binary(16), c_varchar varchar(16), c_nchar nchar(16), c_varbinary varbinary(16));")
        # insert data
        sql = "insert into rt values \
            ('2024-05-08 12:00:00.000', 1, 2, true, 1, 2, 1, 2, 1.1, 111111, 222222, 111111.111111, 'a', 'a', 'a', \"0x01\"), \
            ('2024-05-08 12:00:01.000', 2, 1, false, 2, 1, 2, 1, 2.2, 222222, 111111, 222222.222222, 'b', 'b', 'b', \"0x02\"), \
            ('2024-05-08 12:00:02.000', 3, 3, true, 3, 3, 3, 3, null, 333333, 333333, 3.1111111, 'c', 'c', 'c', \"0x03\"), \
            ('2024-05-08 12:00:03.000', 4, 4, false, 4, 4, 4, 4, 4.4, 444444, 222222, 444444.444444, 'd', 'd', 'd', \"0x04\"), \
            ('2024-05-08 12:00:04.000', 5, 5, true, 5, 5, 5, 5, 5.5, 2, 3, 555555.555555, 'e', 'e', 'e', \"0x05\"), \
            ('2024-05-08 12:00:05.000', 6, 6, false, -5, 5, 5, 5, 5.0, 6, 6, 5, 'e', 'e', 'e', \"0x06\");"
        tdSql.execute(sql)

    def test_notin(self):
        # setChkNotInBytes1
        tdSql.query("select * from rt where c_int8 not in (6, 7);")
        tdSql.checkRows(5)
        tdSql.query("select * from rt where c_int8 not in (1, 2);")
        tdSql.checkRows(4)
        tdSql.query("select * from rt where c_bool not in (true);")
        tdSql.checkRows(3)

        # setChkNotInBytes8
        tdSql.query("select * from rt where c_int64 not in (6666666, 7777777);")
        tdSql.checkRows(6)
        tdSql.query("select * from rt where c_uint64 not in (5555555555);")
        tdSql.checkRows(6)
        tdSql.query("select * from rt where c_double not in (111111.111111, 222222.222222);")
        tdSql.checkRows(4)

        # setChkNotInString
        tdSql.query("select * from rt where c_binary not in ('f', 'g', 'h');")
        tdSql.checkRows(6)
        tdSql.query("select * from rt where c_varchar not in ('a', 'b', 'c');")
        tdSql.checkRows(3)
        tdSql.query("select * from rt where c_nchar not in ('d', 'e', 'f');")
        tdSql.checkRows(3)
        tdSql.query("select * from rt where c_varbinary not in ('0x01', '0x02');")
        tdSql.checkRows(4)

    def test_compare_value(self):
        # compareUint16Val
        tdSql.query("select * from rt where c_uint16 = 5;")
        tdSql.checkRows(2)
        tdSql.query("select * from rt where c_uint16 < 5;")
        tdSql.checkRows(4)

        # compareFloatVal
        tdSql.query("select * from rt where c_float is null;")
        tdSql.checkRows(1)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float < t2.c_float;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float > t2.c_float;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float = t2.c_float;")
        tdSql.checkRows(5)

        # compareDoubleVal
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double = t2.c_double;")
        tdSql.checkRows(6)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double < t2.c_double;")
        tdSql.checkRows(15)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double > t2.c_double;")
        tdSql.checkRows(15)

    def test_compareInt8Int16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 > t2.c_int16;")
        tdSql.checkRows(21)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 < t2.c_int16;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 = t2.c_int16;")
        tdSql.checkRows(5)

    def test_compareInt8Int32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 < t2.c_int32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 > t2.c_int32;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 = t2.c_int32;")
        tdSql.checkRows(6)

    def test_compareInt8Int64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 < t2.c_int64;")
        tdSql.checkRows(30)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 > t2.c_int64;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 = t2.c_int64;")
        tdSql.checkRows(2)

    def test_compareInt8Double(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 < t2.c_double;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 > t2.c_double;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 = t2.c_double;")
        tdSql.checkRows(1)

    def test_compareInt8Uint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 < t2.c_uint8;")
        tdSql.checkRows(15)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 > t2.c_uint8;")
        tdSql.checkRows(15)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 = t2.c_uint8;")
        tdSql.checkRows(6)

    def test_compareInt8Uint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 < t2.c_uint16;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 > t2.c_uint16;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 = t2.c_uint16;")
        tdSql.checkRows(6)

    def test_compareInt8Uint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 < t2.c_uint32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 > t2.c_uint32;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 = t2.c_uint32;")
        tdSql.checkRows(6)
    
    def test_compareInt8Uint64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 < t2.c_uint64;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 > t2.c_uint64;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int8 = t2.c_uint64;")
        tdSql.checkRows(2)

    def test_compareInt16Int32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 < t2.c_int32;")
        tdSql.checkRows(20)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 > t2.c_int32;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 = t2.c_int32;")
        tdSql.checkRows(6)

    def test_compareInt16Int64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 < t2.c_int64;")
        tdSql.checkRows(32)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 > t2.c_int64;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 = t2.c_int32;")
        tdSql.checkRows(6)

    def test_compareInt16Double(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 < t2.c_double;")
        tdSql.checkRows(33)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 > t2.c_double;")
        tdSql.checkRows(2)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 = t2.c_double;")
        tdSql.checkRows(1)

    def test_compareInt16Uint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 < t2.c_uint8;")
        tdSql.checkRows(21)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 > t2.c_uint8;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 = t2.c_uint8;")
        tdSql.checkRows(5)

    def test_compareInt16Uint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 < t2.c_uint16;")
        tdSql.checkRows(20)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 > t2.c_uint16;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 = t2.c_uint16;")
        tdSql.checkRows(6)
    
    def test_compareInt16Uint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 < t2.c_uint32;")
        tdSql.checkRows(20)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 > t2.c_uint32;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 = t2.c_uint32;")
        tdSql.checkRows(6)

    def test_compareInt16Uint64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 < t2.c_uint64;")
        tdSql.checkRows(33)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 > t2.c_uint64;")
        tdSql.checkRows(2)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int16 = t2.c_uint64;")
        tdSql.checkRows(1)

    def test_compareInt32Int16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 < t2.c_int16;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 > t2.c_int16;")
        tdSql.checkRows(20)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 = t2.c_int16;")
        tdSql.checkRows(6)

    def test_compareInt32Int64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 < t2.c_int64;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 > t2.c_int64;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 = t2.c_int64;")
        tdSql.checkRows(1)

    def test_compareInt32Float(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 < t2.c_float;")
        tdSql.checkRows(17)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 > t2.c_float;")
        tdSql.checkRows(11)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 = t2.c_float;")
        tdSql.checkRows(2)

    def test_compareInt32Uint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 < t2.c_uint8;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 > t2.c_uint8;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 = t2.c_uint8;")
        tdSql.checkRows(6)

    def test_compareInt32Uint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 < t2.c_uint16;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 > t2.c_uint16;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 = t2.c_uint16;")
        tdSql.checkRows(8)

    def test_compareInt32Uint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 < t2.c_uint32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 > t2.c_uint32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 = t2.c_uint32;")
        tdSql.checkRows(8)

    def test_compareInt32Uint64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 < t2.c_uint64;")
        tdSql.checkRows(32)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 > t2.c_uint64;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int32 = t2.c_uint64;")
        tdSql.checkRows(1)

    def test_compareInt64Uint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 < t2.c_uint8;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 > t2.c_uint8;")
        tdSql.checkRows(30)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 = t2.c_uint8;")
        tdSql.checkRows(2)

    def test_compareInt64Uint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 < t2.c_uint16;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 > t2.c_uint16;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 = t2.c_uint16;")
        tdSql.checkRows(1)

    def test_compareInt64Uint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 < t2.c_uint32;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 > t2.c_uint32;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_int64 = t2.c_uint32;")
        tdSql.checkRows(1)

    def test_compareFloatInt32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float < t2.c_int32;")
        tdSql.checkRows(11)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float > t2.c_int32;")
        tdSql.checkRows(17)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float = t2.c_int32;")
        tdSql.checkRows(2)

    def test_compareFloatUint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float < t2.c_uint8;")
        tdSql.checkRows(13)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float > t2.c_uint8;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float = t2.c_uint8;")
        tdSql.checkRows(1)

    def test_compareFloatUint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float < t2.c_uint16;")
        tdSql.checkRows(11)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float > t2.c_uint16;")
        tdSql.checkRows(17)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float = t2.c_uint16;")
        tdSql.checkRows(2)

    def test_compareFloatUint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float < t2.c_uint32;")
        tdSql.checkRows(11)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float > t2.c_uint32;")
        tdSql.checkRows(17)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float = t2.c_uint32;")
        tdSql.checkRows(2)

    def test_compareFloatUint64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float < t2.c_uint64;")
        tdSql.checkRows(27)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float > t2.c_uint64;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_float = t2.c_uint64;")
        tdSql.checkRows(0)

    def test_compareDoubleUint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double < t2.c_uint8;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double > t2.c_uint8;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double = t2.c_uint8;")
        tdSql.checkRows(1)

    def test_compareDoubleUint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double < t2.c_uint16;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double > t2.c_uint16;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double = t2.c_uint16;")
        tdSql.checkRows(2)

    def test_compareDoubleUint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double < t2.c_uint32;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double > t2.c_uint32;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double = t2.c_uint32;")
        tdSql.checkRows(2)

    def test_compareDoubleUint64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double < t2.c_uint64;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double > t2.c_uint64;")
        tdSql.checkRows(22)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_double = t2.c_uint64;")
        tdSql.checkRows(0)

    def test_compareUint8Int16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 < t2.c_int16;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 > t2.c_int16;")
        tdSql.checkRows(21)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 = t2.c_int16;")
        tdSql.checkRows(5)

    def test_compareUint8Int32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 < t2.c_int32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 > t2.c_int32;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 = t2.c_int32;")
        tdSql.checkRows(6)

    def test_compareUint8Int64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 < t2.c_int64;")
        tdSql.checkRows(30)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 > t2.c_int64;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 = t2.c_int64;")
        tdSql.checkRows(2)

    def test_compareUint8Float(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 < t2.c_float;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 > t2.c_float;")
        tdSql.checkRows(13)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 = t2.c_float;")
        tdSql.checkRows(1)

    def test_compareUint8Double(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 < t2.c_double;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 > t2.c_double;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 = t2.c_double;")
        tdSql.checkRows(1)

    def test_compareUint8Uint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 < t2.c_uint16;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 > t2.c_uint16;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 = t2.c_uint16;")
        tdSql.checkRows(6)

    def test_compareUint8Uint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 < t2.c_uint32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 > t2.c_uint32;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 = t2.c_uint32;")
        tdSql.checkRows(6)

    def test_compareUint8Uint64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 < t2.c_uint64;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 > t2.c_uint64;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint8 = t2.c_uint64;")
        tdSql.checkRows(2)

    def test_compareUint16Int16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 < t2.c_int16;")
        tdSql.checkRows(10)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 > t2.c_int16;")
        tdSql.checkRows(20)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 = t2.c_int16;")
        tdSql.checkRows(6)

    def test_compareUint16Int32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 < t2.c_int32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 > t2.c_int32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 = t2.c_int32;")
        tdSql.checkRows(8)

    def test_compareUint16Int64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 < t2.c_int64;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 > t2.c_int64;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 = t2.c_int64;")
        tdSql.checkRows(1)

    def test_compareUint16Float(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 < t2.c_float;")
        tdSql.checkRows(17)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 > t2.c_float;")
        tdSql.checkRows(11)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 = t2.c_float;")
        tdSql.checkRows(2)

    def test_compareUint16Double(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 < t2.c_double;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 > t2.c_double;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 = t2.c_double;")
        tdSql.checkRows(2)

    def test_compareUint16Uint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 < t2.c_uint8;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 > t2.c_uint8;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 = t2.c_uint8;")
        tdSql.checkRows(6)

    def test_compareUint16Uint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 < t2.c_uint32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 > t2.c_uint32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint16 = t2.c_uint32;")
        tdSql.checkRows(8)

    def test_compareUint32Int32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 < t2.c_int32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 > t2.c_int32;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 = t2.c_int32;")
        tdSql.checkRows(8)

    def test_compareUint32Int64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 < t2.c_int64;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 > t2.c_int64;")
        tdSql.checkRows(4)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 = t2.c_int64;")
        tdSql.checkRows(1)

    def test_compareUint32Float(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 < t2.c_float;")
        tdSql.checkRows(17)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 > t2.c_float;")
        tdSql.checkRows(11)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 = t2.c_float;")
        tdSql.checkRows(2)

    def test_compareUint32Double(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 < t2.c_double;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 > t2.c_double;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 = t2.c_double;")
        tdSql.checkRows(2)

    def test_compareUint32Uint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 < t2.c_uint8;")
        tdSql.checkRows(16)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 > t2.c_uint8;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 = t2.c_uint8;")
        tdSql.checkRows(6)

    def test_compareUint32Uint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 < t2.c_uint16;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 > t2.c_uint16;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 = t2.c_uint16;")
        tdSql.checkRows(8)

    def test_compareUint32Uint64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 < t2.c_uint64;")
        tdSql.checkRows(32)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 > t2.c_uint64;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint32 = t2.c_uint64;")
        tdSql.checkRows(1)

    def test_compareUint64Int16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 < t2.c_int16;")
        tdSql.checkRows(2)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 > t2.c_int16;")
        tdSql.checkRows(33)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 = t2.c_int16;")
        tdSql.checkRows(1)

    def test_compareUint64Int32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 < t2.c_int32;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 > t2.c_int32;")
        tdSql.checkRows(32)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 = t2.c_int32;")
        tdSql.checkRows(1)

    def test_compareUint64Int64(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 < t2.c_int64;")
        tdSql.checkRows(17)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 > t2.c_int64;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 = t2.c_int64;")
        tdSql.checkRows(5)

    def test_compareUint64Float(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 < t2.c_float;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 > t2.c_float;")
        tdSql.checkRows(27)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 = t2.c_float;")
        tdSql.checkRows(0)

    def test_compareUint64Double(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 < t2.c_double;")
        tdSql.checkRows(22)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 > t2.c_double;")
        tdSql.checkRows(14)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 = t2.c_double;")
        tdSql.checkRows(0)

    def test_compareUint64Uint8(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 < t2.c_uint8;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 > t2.c_uint8;")
        tdSql.checkRows(31)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 = t2.c_uint8;")
        tdSql.checkRows(2)

    def test_compareUint64Uint16(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 < t2.c_uint16;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 > t2.c_uint16;")
        tdSql.checkRows(32)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 = t2.c_uint16;")
        tdSql.checkRows(1)

    def test_compareUint64Uint32(self):
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 < t2.c_uint32;")
        tdSql.checkRows(3)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 > t2.c_uint32;")
        tdSql.checkRows(32)
        tdSql.query("select * from rt t1 left join rt t2 on timetruncate(t1.ts, 1m) = timetruncate(t2.ts, 1m) where t1.c_uint64 = t2.c_uint32;")
        tdSql.checkRows(1)

    def run(self):
        self.prepare_data()
        self.test_notin()
        self.test_compare_value()
        self.test_compareInt8Int16()
        self.test_compareInt8Int32()
        self.test_compareInt8Int64()
        self.test_compareInt8Double()
        self.test_compareInt8Uint8()
        self.test_compareInt8Uint16()
        self.test_compareInt8Uint32()
        self.test_compareInt8Uint64()
        self.test_compareInt16Int32()
        self.test_compareInt16Int64()
        self.test_compareInt16Double()
        self.test_compareInt16Uint8()
        self.test_compareInt16Uint16()
        self.test_compareInt16Uint32()
        self.test_compareInt16Uint64()
        self.test_compareInt32Int16()
        self.test_compareInt32Int64()
        self.test_compareInt32Float()
        self.test_compareInt32Uint8()
        self.test_compareInt32Uint16()
        self.test_compareInt32Uint32()
        self.test_compareInt32Uint64()
        self.test_compareInt64Uint8()
        self.test_compareInt64Uint16()
        self.test_compareInt64Uint32()
        self.test_compareFloatInt32()
        self.test_compareFloatUint8()
        self.test_compareFloatUint16()
        self.test_compareFloatUint32()
        self.test_compareFloatUint64()
        self.test_compareDoubleUint8()
        self.test_compareDoubleUint16()
        self.test_compareDoubleUint32()
        self.test_compareDoubleUint64()
        self.test_compareUint8Int16()
        self.test_compareUint8Int32()
        self.test_compareUint8Int64()
        self.test_compareUint8Float()
        self.test_compareUint8Double()
        self.test_compareUint8Uint16()
        self.test_compareUint8Uint32()
        self.test_compareUint8Uint64()
        self.test_compareUint16Int16()
        self.test_compareUint16Int32()
        self.test_compareUint16Int64()
        self.test_compareUint16Float()
        self.test_compareUint16Double()
        self.test_compareUint16Uint8()
        self.test_compareUint16Uint32()
        self.test_compareUint32Int32()
        self.test_compareUint32Int64()
        self.test_compareUint32Float()
        self.test_compareUint32Double()
        self.test_compareUint32Uint8()
        self.test_compareUint32Uint16()
        self.test_compareUint32Uint64()
        self.test_compareUint64Int16()
        self.test_compareUint64Int32()
        self.test_compareUint64Int64()
        self.test_compareUint64Float()
        self.test_compareUint64Double()
        self.test_compareUint64Uint8()
        self.test_compareUint64Uint16()
        self.test_compareUint64Uint32()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
