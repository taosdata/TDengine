###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.types import TDSmlProtocolType, TDSmlTimestampType

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database if not exists test precision 'us'")
        tdSql.execute('use test')

        ### metric ###
        print("============= step1 : test metric  ================")
        lines0 = [
                        "stb0_0 1626006833639 4i8 host=\"host0\" interface=\"eth0\"",
                        "stb0_1 1626006833639 4i8 host=\"host0\" interface=\"eth0\"",
                        "stb0_2 1626006833639 4i8 host=\"host0\" interface=\"eth0\"",
                        ".stb0.3. 1626006833639 4i8 host=\"host0\" interface=\"eth0\"",
                   ]

        code = self._conn.schemaless_insert(lines0, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("show stables")
        tdSql.checkRows(4)

        tdSql.query("describe stb0_0")
        tdSql.checkRows(4)

        tdSql.query("describe stb0_1")
        tdSql.checkRows(4)

        tdSql.query("describe stb0_2")
        tdSql.checkRows(4)

        tdSql.query("describe `.stb0.3.`")
        tdSql.checkRows(4)

        ### timestamp ###
        print("============= step2 : test timestamp  ================")
        lines1 = [
                      "stb1 1626006833641 1i8 host=\"host0\"",
                      "stb1 1626006834 2i8 host=\"host0\"",
                      "stb1 0 3i8 host=\"host0\"",
                    ]

        code = self._conn.schemaless_insert(lines1, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb1")
        tdSql.checkRows(3)

        ### metric value ###
        print("============= step3 : test metric value  ================")

        #tinyint
        lines2_0 = [
                        "stb2_0 1626006833651 -127i8 host=\"host0\"",
                        "stb2_0 1626006833652 127i8 host=\"host0\""
                     ]
        code = self._conn.schemaless_insert(lines2_0, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_0")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_0")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "TINYINT")

        #smallint
        lines2_1 = [
                        "stb2_1 1626006833651 -32767i16 host=\"host0\"",
                        "stb2_1 1626006833652 32767i16 host=\"host0\""
                     ]
        code = self._conn.schemaless_insert(lines2_1, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_1")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_1")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "SMALLINT")

        #int
        lines2_2 = [
                        "stb2_2 1626006833651 -2147483647i32 host=\"host0\"",
                        "stb2_2 1626006833652 2147483647i32 host=\"host0\""
                     ]

        code = self._conn.schemaless_insert(lines2_2, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_2")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_2")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "INT")

        #bigint
        lines2_3 = [
                        "stb2_3 1626006833651 -9223372036854775807i64 host=\"host0\"",
                        "stb2_3 1626006833652 9223372036854775807i64 host=\"host0\""
                     ]

        code = self._conn.schemaless_insert(lines2_3, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_3")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_3")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "BIGINT")

        #float
        lines2_4 = [
                        "stb2_4 1626006833610 3f32 host=\"host0\"",
                        "stb2_4 1626006833620 -3f32 host=\"host0\"",
                        "stb2_4 1626006833630 3.4f32 host=\"host0\"",
                        "stb2_4 1626006833640 -3.4f32 host=\"host0\"",
                        "stb2_4 1626006833650 3.4E10f32 host=\"host0\"",
                        "stb2_4 1626006833660 -3.4e10f32 host=\"host0\"",
                        "stb2_4 1626006833670 3.4E+2f32 host=\"host0\"",
                        "stb2_4 1626006833680 -3.4e-2f32 host=\"host0\"",
                        "stb2_4 1626006833700 3.4E38f32 host=\"host0\"",
                        "stb2_4 1626006833710 -3.4E38f32 host=\"host0\""
                     ]

        code = self._conn.schemaless_insert(lines2_4, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_4")
        tdSql.checkRows(10)

        tdSql.query("describe stb2_4")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "FLOAT")

        #double
        lines2_5 = [
                        "stb2_5 1626006833610 3f64 host=\"host0\"",
                        "stb2_5 1626006833620 -3f64 host=\"host0\"",
                        "stb2_5 1626006833630 3.4f64 host=\"host0\"",
                        "stb2_5 1626006833640 -3.4f64 host=\"host0\"",
                        "stb2_5 1626006833650 3.4E10f64 host=\"host0\"",
                        "stb2_5 1626006833660 -3.4e10f64 host=\"host0\"",
                        "stb2_5 1626006833670 3.4E+2f64 host=\"host0\"",
                        "stb2_5 1626006833680 -3.4e-2f64 host=\"host0\"",
                        "stb2_5 1626006833690 1.7E308f64 host=\"host0\"",
                        "stb2_5 1626006833700 -1.7E308f64 host=\"host0\"",
                        "stb2_5 1626006833710 3 host=\"host0\""
                     ]

        code = self._conn.schemaless_insert(lines2_5, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_5")
        tdSql.checkRows(11)

        tdSql.query("describe stb2_5")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "DOUBLE")

        #bool
        lines2_6 = [
                        "stb2_6 1626006833610 t host=\"host0\"",
                        "stb2_6 1626006833620 T host=\"host0\"",
                        "stb2_6 1626006833630 true host=\"host0\"",
                        "stb2_6 1626006833640 True host=\"host0\"",
                        "stb2_6 1626006833650 TRUE host=\"host0\"",
                        "stb2_6 1626006833660 f host=\"host0\"",
                        "stb2_6 1626006833670 F host=\"host0\"",
                        "stb2_6 1626006833680 false host=\"host0\"",
                        "stb2_6 1626006833690 False host=\"host0\"",
                        "stb2_6 1626006833700 FALSE host=\"host0\""
                     ]

        code = self._conn.schemaless_insert(lines2_6, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_6")
        tdSql.checkRows(10)

        tdSql.query("describe stb2_6")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "BOOL")

        #binary
        lines2_7 = [
                        "stb2_7 1626006833610 \"  binary_val  .!@#$%^&*  \" host=\"host0\"",
                        "stb2_7 1626006833620 \"binary_val.:;,./?|+-=\" host=\"host0\"",
                        "stb2_7 1626006833630 \"binary_val.()[]{}<>\" host=\"host0\""
                     ]

        code = self._conn.schemaless_insert(lines2_7, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_7")
        tdSql.checkRows(3)

        tdSql.query("describe stb2_7")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "BINARY")

        #nchar
        lines2_8 = [
                        "stb2_8 1626006833610 L\"  nchar_val  数值一  \" host=\"host0\"",
                        "stb2_8 1626006833620 L\"nchar_val数值二\" host=\"host0\""
                     ]

        code = self._conn.schemaless_insert(lines2_8, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb2_8")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_8")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "NCHAR")

        ### tags ###
        print("============= step3 : test tags  ================")
        #tag value types
        lines3_0 = [
                        "stb3_0 1626006833610 1 t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=3.4E38f32 t6=1.7E308f64 t7=true t8=\"binary_val_1\" t9=L\"标签值1\"",
                        "stb3_0 1626006833610 2 t1=-127i8 t2=-32767i16 t3=-2147483647i32 t4=-9223372036854775807i64 t5=-3.4E38f32 t6=-1.7E308f64 t7=false t8=\"binary_val_2\" t9=L\"标签值2\""
                     ]

        code = self._conn.schemaless_insert(lines3_0, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from stb3_0")
        tdSql.checkRows(2)

        tdSql.query("describe stb3_0")
        tdSql.checkRows(11)

        tdSql.checkData(2, 1, "NCHAR")
        tdSql.checkData(2, 3, "TAG")

        tdSql.checkData(3, 1, "NCHAR")
        tdSql.checkData(3, 3, "TAG")

        tdSql.checkData(4, 1, "NCHAR")
        tdSql.checkData(4, 3, "TAG")

        tdSql.checkData(5, 1, "NCHAR")
        tdSql.checkData(5, 3, "TAG")

        tdSql.checkData(6, 1, "NCHAR")
        tdSql.checkData(6, 3, "TAG")

        tdSql.checkData(7, 1, "NCHAR")
        tdSql.checkData(7, 3, "TAG")

        tdSql.checkData(8, 1, "NCHAR")
        tdSql.checkData(8, 3, "TAG")

        tdSql.checkData(9, 1, "NCHAR")
        tdSql.checkData(9, 3, "TAG")

        tdSql.checkData(10, 1, "NCHAR")
        tdSql.checkData(10, 3, "TAG")


        #tag ID as child table name
        #lines3_1 = [
        #                "stb3_1 1626006833610 1 id=child_table1 host=host1",
        #                "stb3_1 1626006833610 2 host=host2 iD=child_table2",
        #                "stb3_1 1626006833610 3 ID=child_table3 host=host3"
        #             ]

        #code = self._conn.schemaless_insert(lines3_1, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        #print("schemaless_insert result {}".format(code))

        #tdSql.query("select * from stb3_1")
        #tdSql.checkRows(3)

        #tdSql.query("show tables like \"child%\"")
        #tdSql.checkRows(3)

        #tdSql.checkData(0, 0, "child_table1")

        ### special characters and keywords ###
        print("============= step4 : test special characters and keywords  ================")
        lines4_1 = [
                        "1234 1626006833610 1 id=123 456=true int=true double=false into=1 from=2 !@#$.%^&*()=false",
                        "int 1626006833610 2 id=and 456=true int=true double=false into=1 from=2 !@#$.%^&*()=false",
                        "double 1626006833610 2 id=for 456=true int=true double=false into=1 from=2 !@#$.%^&*()=false",
                        "from 1626006833610 2 id=!@#.^& 456=true int=true double=false into=1 from=2 !@#$.%^&*()=false",
                        "!@#$.%^&*() 1626006833610 2 id=none 456=true int=true double=false into=1 from=2 !@#$.%^&*()=false",
                        "STABLE 1626006833610 2 id=KEY 456=true int=true double=false TAG=1 FROM=2 COLUMN=false",
                     ]

        code = self._conn.schemaless_insert(lines4_1, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query('describe `1234`')
        tdSql.checkRows(9)

        tdSql.query('describe `int`')
        tdSql.checkRows(9)

        tdSql.query('describe `double`')
        tdSql.checkRows(9)

        tdSql.query('describe `from`')
        tdSql.checkRows(9)

        tdSql.query('describe `!@#$.%^&*()`')
        tdSql.checkRows(9)

        tdSql.query('describe `STABLE`')
        tdSql.checkRows(9)

        #tdSql.query('select * from `123`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `and`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `for`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `!@#.^&`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `none`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `key`')
        #tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
