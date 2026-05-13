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
                        "stb0_0 1626006833639000000ns 4i8 host=\"host0\",interface=\"eth0\"",
                        "stb0_1 1626006833639000000ns 4i8 host=\"host0\",interface=\"eth0\"",
                        "stb0_2 1626006833639000000ns 4i8 host=\"host0\",interface=\"eth0\"",
                   ]

        code = self._conn.insert_telnet_lines(lines0)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("show stables")
        tdSql.checkRows(3)

        tdSql.query("describe stb0_0")
        tdSql.checkRows(4)

        tdSql.query("describe stb0_1")
        tdSql.checkRows(4)

        tdSql.query("describe stb0_2")
        tdSql.checkRows(4)

        ### timestamp ###
        print("============= step2 : test timestamp  ================")
        lines1 = [
                      "stb1 1626006833s 1i8 host=\"host0\"",
                      "stb1 1626006833639000000ns 2i8 host=\"host0\"",
                      "stb1 1626006833640000us 3i8 host=\"host0\"",
                      "stb1 1626006833641123 4i8 host=\"host0\"",
                      "stb1 1626006833651ms 5i8 host=\"host0\"",
                      "stb1 0 6i8 host=\"host0\"",
                    ]

        code = self._conn.insert_telnet_lines(lines1)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb1")
        tdSql.checkRows(6)

        ### metric value ###
        print("============= step3 : test metric value  ================")

        #tinyint
        lines2_0 = [
                        "stb2_0 1626006833651ms -127i8 host=\"host0\"",
                        "stb2_0 1626006833652ms 127i8 host=\"host0\""
                     ]
        code = self._conn.insert_telnet_lines(lines2_0)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_0")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_0")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "TINYINT")

        #smallint
        lines2_1 = [
                        "stb2_1 1626006833651ms -32767i16 host=\"host0\"",
                        "stb2_1 1626006833652ms 32767i16 host=\"host0\""
                     ]
        code = self._conn.insert_telnet_lines(lines2_1)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_1")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_1")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "SMALLINT")

        #int
        lines2_2 = [
                        "stb2_2 1626006833651ms -2147483647i32 host=\"host0\"",
                        "stb2_2 1626006833652ms 2147483647i32 host=\"host0\""
                     ]

        code = self._conn.insert_telnet_lines(lines2_2)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_2")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_2")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "INT")

        #bigint
        lines2_3 = [
                        "stb2_3 1626006833651ms -9223372036854775807i64 host=\"host0\"",
                        "stb2_3 1626006833652ms 9223372036854775807i64 host=\"host0\""
                     ]

        code = self._conn.insert_telnet_lines(lines2_3)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_3")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_3")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "BIGINT")

        #float
        lines2_4 = [
                        "stb2_4 1626006833610ms 3f32 host=\"host0\"",
                        "stb2_4 1626006833620ms -3f32 host=\"host0\"",
                        "stb2_4 1626006833630ms 3.4f32 host=\"host0\"",
                        "stb2_4 1626006833640ms -3.4f32 host=\"host0\"",
                        "stb2_4 1626006833650ms 3.4E10f32 host=\"host0\"",
                        "stb2_4 1626006833660ms -3.4e10f32 host=\"host0\"",
                        "stb2_4 1626006833670ms 3.4E+2f32 host=\"host0\"",
                        "stb2_4 1626006833680ms -3.4e-2f32 host=\"host0\"",
                        "stb2_4 1626006833690ms 3.15 host=\"host0\"",
                        "stb2_4 1626006833700ms 3.4E38f32 host=\"host0\"",
                        "stb2_4 1626006833710ms -3.4E38f32 host=\"host0\""
                     ]

        code = self._conn.insert_telnet_lines(lines2_4)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_4")
        tdSql.checkRows(11)

        tdSql.query("describe stb2_4")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "FLOAT")

        #double
        lines2_5 = [
                        "stb2_5 1626006833610ms 3f64 host=\"host0\"",
                        "stb2_5 1626006833620ms -3f64 host=\"host0\"",
                        "stb2_5 1626006833630ms 3.4f64 host=\"host0\"",
                        "stb2_5 1626006833640ms -3.4f64 host=\"host0\"",
                        "stb2_5 1626006833650ms 3.4E10f64 host=\"host0\"",
                        "stb2_5 1626006833660ms -3.4e10f64 host=\"host0\"",
                        "stb2_5 1626006833670ms 3.4E+2f64 host=\"host0\"",
                        "stb2_5 1626006833680ms -3.4e-2f64 host=\"host0\"",
                        "stb2_5 1626006833690ms 1.7E308f64 host=\"host0\"",
                        "stb2_5 1626006833700ms -1.7E308f64 host=\"host0\""
                     ]

        code = self._conn.insert_telnet_lines(lines2_5)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_5")
        tdSql.checkRows(10)

        tdSql.query("describe stb2_5")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "DOUBLE")

        #bool
        lines2_6 = [
                        "stb2_6 1626006833610ms t host=\"host0\"",
                        "stb2_6 1626006833620ms T host=\"host0\"",
                        "stb2_6 1626006833630ms true host=\"host0\"",
                        "stb2_6 1626006833640ms True host=\"host0\"",
                        "stb2_6 1626006833650ms TRUE host=\"host0\"",
                        "stb2_6 1626006833660ms f host=\"host0\"",
                        "stb2_6 1626006833670ms F host=\"host0\"",
                        "stb2_6 1626006833680ms false host=\"host0\"",
                        "stb2_6 1626006833690ms False host=\"host0\"",
                        "stb2_6 1626006833700ms FALSE host=\"host0\""
                     ]

        code = self._conn.insert_telnet_lines(lines2_6)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_6")
        tdSql.checkRows(10)

        tdSql.query("describe stb2_6")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "BOOL")

        #binary
        lines2_7 = [
                        "stb2_7 1626006833610ms \"binary_val.!@#$%^&*\" host=\"host0\"",
                        "stb2_7 1626006833620ms \"binary_val.:;,./?|+-=\" host=\"host0\"",
                        "stb2_7 1626006833630ms \"binary_val.()[]{}<>\" host=\"host0\""
                     ]

        code = self._conn.insert_telnet_lines(lines2_7)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_7")
        tdSql.checkRows(3)

        tdSql.query("describe stb2_7")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "BINARY")

        #nchar
        lines2_8 = [
                        "stb2_8 1626006833610ms L\"nchar_val数值一\" host=\"host0\"",
                        "stb2_8 1626006833620ms L\"nchar_val数值二\" host=\"host0\""
                     ]

        code = self._conn.insert_telnet_lines(lines2_8)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb2_8")
        tdSql.checkRows(2)

        tdSql.query("describe stb2_8")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "NCHAR")

        ### tags ###
        print("============= step3 : test tags  ================")
        #tag value types
        lines3_0 = [
                        "stb3_0 1626006833610ms 1 t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=3.4E38f32,t6=1.7E308f64,t7=true,t8=\"binary_val_1\",t9=L\"标签值1\"",
                        "stb3_0 1626006833610ms 2 t1=-127i8,t2=-32767i16,t3=-2147483647i32,t4=-9223372036854775807i64,t5=-3.4E38f32,t6=-1.7E308f64,t7=false,t8=\"binary_val_2\",t9=L\"标签值2\""
                     ]

        code = self._conn.insert_telnet_lines(lines3_0)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb3_0")
        tdSql.checkRows(2)

        tdSql.query("describe stb3_0")
        tdSql.checkRows(11)

        tdSql.checkData(2, 1, "TINYINT")
        tdSql.checkData(2, 3, "TAG")

        tdSql.checkData(3, 1, "SMALLINT")
        tdSql.checkData(3, 3, "TAG")

        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(4, 3, "TAG")

        tdSql.checkData(5, 1, "BIGINT")
        tdSql.checkData(5, 3, "TAG")

        tdSql.checkData(6, 1, "FLOAT")
        tdSql.checkData(6, 3, "TAG")

        tdSql.checkData(7, 1, "DOUBLE")
        tdSql.checkData(7, 3, "TAG")

        tdSql.checkData(8, 1, "BOOL")
        tdSql.checkData(8, 3, "TAG")

        tdSql.checkData(9, 1, "BINARY")
        tdSql.checkData(9, 3, "TAG")

        tdSql.checkData(10, 1, "NCHAR")
        tdSql.checkData(10, 3, "TAG")


        #tag ID as child table name
        lines3_1 = [
                        "stb3_1 1626006833610ms 1 id=\"child_table1\",host=\"host1\"",
                        "stb3_1 1626006833610ms 2 host=\"host2\",iD=\"child_table2\"",
                        "stb3_1 1626006833610ms 3 ID=\"child_table3\",host=\"host3\""
                     ]

        code = self._conn.insert_telnet_lines(lines3_1)
        print("insert_telnet_lines result {}".format(code))

        tdSql.query("select * from stb3_1")
        tdSql.checkRows(3)

        tdSql.query("show tables like \"child%\"")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "child_table1")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
