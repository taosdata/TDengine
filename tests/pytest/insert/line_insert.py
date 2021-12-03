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

        tdSql.execute('create stable ste(ts timestamp, f int) tags(t1 bigint)')

        lines = [   "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"\"\"a    pa,\"s   si,t \"\"\",c2=false,c4=4f64 1626006833639000000",
                    "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000",
                    "ste,t2=5f64,t3=L\"ste\" c1=true,c2=4i64,c3=\" i,\"a \"m,\"\"\" 1626056811823316532",
                    "stf,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000",
                    "st,t1=4i64,t2=5f64,t3=\"t4\" c1=3i64,c3=L\"passitagain\",c2=true,c4=5f64 1626006833642000000",
                    "ste,t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false 1626056811843316532",
                    "ste,t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false,c5=32i8,c6=64i16,c7=32i32,c8=88.88f32 1626056812843316532",
                    "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000",
                    "stf,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933641000000"
                ]

        code = self._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        lines2 = [  "stg,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                    "stg,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"
                ]

        code = self._conn.schemaless_insert([ lines2[0] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        code = self._conn.schemaless_insert([ lines2[1] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select * from st")
        tdSql.checkRows(4)

        tdSql.query("select * from ste")
        tdSql.checkRows(3)

        tdSql.query("select * from stf")
        tdSql.checkRows(2)

        tdSql.query("select * from stg")
        tdSql.checkRows(2)

        tdSql.query("show tables")
        tdSql.checkRows(8)

        tdSql.query("describe stf")
        tdSql.checkData(2, 2, 14)

        self._conn.schemaless_insert([
                                "sth,t1=4i64,t2=5f64,t4=5f64,ID=childtable c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933641",
                                "sth,t1=4i64,t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933654"
                                ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value)
        tdSql.execute('reset query cache')

        tdSql.query('select tbname, * from sth')
        tdSql.checkRows(2)

        #tdSql.query('select tbname, * from childtable')
        #tdSql.checkRows(1)

        ###Test when tag is omitted
        lines3 = [  "sti c1=4i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                    "sti c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"
                ]

        code = self._conn.schemaless_insert(lines3, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query('select * from sti')
        tdSql.checkRows(2)

        tdSql.query('select tbname from sti')
        tdSql.checkRows(1)

        lines4 = [  "stp c1=4i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                    "stp c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"
                ]
        code = self._conn.schemaless_insert([ lines4[0] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        code = self._conn.schemaless_insert([ lines4[1] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query('select * from stp')
        tdSql.checkRows(2)

        tdSql.query('select tbname from stp')
        tdSql.checkRows(1)

        lines5 = [  "stq c1=4i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                    "stq,t1=abc c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000",
                    "stq,t2=abc c1=3i64,c3=L\"passitagin\",c4=5f64,c5=5f64,c6=true 1626006833640000000"
                ]
        code = self._conn.schemaless_insert([ lines5[0] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        code = self._conn.schemaless_insert([ lines5[1] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        code = self._conn.schemaless_insert([ lines5[2] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query('select * from stq')
        tdSql.checkRows(3)

        tdSql.query('select tbname from stq')
        tdSql.checkRows(3)

        lines6 = [  "str c1=4i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                    "str,t1=abc c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000",
                    "str,t2=abc c1=3i64,c3=L\"passitagin\",c4=5f64,c5=5f64,c6=true 1626006833640000000"
                ]
        code = self._conn.schemaless_insert(lines6, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query('select * from str')
        tdSql.checkRows(3)

        tdSql.query('select tbname from str')
        tdSql.checkRows(3)

        ###Special Character and keyss
        self._conn.schemaless_insert([
                                "1234,id=3456,abc=4i64,def=3i64 123=3i64,int=2i64,bool=false,into=5f64,column=7u64,!@#$.%^&*()=false 1626006933641",
                                "int,id=and,123=4i64,smallint=5f64,double=5f64,of=3i64,key=L\"passitagin_stf\",!@#$.%^&*()=false abc=false 1626006933654",
                                "double,id=for,123=4i64,smallint=5f64,double=5f64,of=3i64,key=L\"passitagin_stf\",!@#$.%^&*()=false abc=false 1626006933664",
                                "from,id=!@#$.%^,123=4i64,smallint=5f64,double=5f64,of=3i64,key=L\"passitagin_stf\",!@#$.%^&*()=false abc=false 1626006933674",
                                "!@#$.%^&*(),id=none,123=4i64,smallint=5f64,double=5f64,of=3i64,key=L\"passitagin_stf\",!@#$.%^&*()=false abc=false 1626006933684",
                                "STABLE,id=CREATE,123=4i64,smallint=5f64,DOUBLE=5f64,of=3i64,key=L\"passitagin_stf\",!@#$.%^&*()=false SELECT=false 1626006933684",
                                ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value)
        tdSql.execute('reset query cache')

        tdSql.query('describe `1234`')
        tdSql.checkRows(10)

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

        #tdSql.query('select * from `3456`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `and`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `for`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `!@#$.%^`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `none`')
        #tdSql.checkRows(1)

        #tdSql.query('select * from `create`')
        #tdSql.checkRows(1)
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
