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


        ### Default format ###
        ### metric ###
        print("============= step0 : test metric  ================")
        payload = ['''
        {
	    "metric":	".stb.0.",
	    "timestamp":	1626006833610,
	    "value":	10,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe `.stb.0.`")
        tdSql.checkRows(6)

        ### metric value ###
        print("============= step1 : test metric value types  ================")
        payload = ['''
        {
	    "metric":	"stb0_0",
	    "timestamp":	1626006833610,
	    "value":	10,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_0")
        tdSql.checkData(1, 1, "DOUBLE")

        payload = ['''
        {
	    "metric":	"stb0_1",
	    "timestamp":	1626006833610,
	    "value":	true,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_1")
        tdSql.checkData(1, 1, "BOOL")

        payload = ['''
        {
	    "metric":	"stb0_2",
	    "timestamp":	1626006833610,
	    "value":	false,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_2")
        tdSql.checkData(1, 1, "BOOL")

        payload = ['''
        {
	    "metric":	"stb0_3",
	    "timestamp":	1626006833610,
	    "value":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>",
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_3")
        tdSql.checkData(1, 1, "NCHAR")

        payload = ['''
        {
	    "metric":	"stb0_4",
	    "timestamp":	1626006833610,
	    "value":	3.14,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_4")
        tdSql.checkData(1, 1, "DOUBLE")

        payload = ['''
        {
	    "metric":	"stb0_5",
	    "timestamp":	1626006833610,
	    "value":	3.14E-2,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_5")
        tdSql.checkData(1, 1, "DOUBLE")


        print("============= step2 : test timestamp  ================")
        ### timestamp 0 ###
        payload = ['''
        {
	    "metric":	"stb0_6",
	    "timestamp":	0,
	    "value":	123,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        ### timestamp 10 digits second ###
        payload = ['''
        {
	    "metric":	"stb0_7",
	    "timestamp":	1626006833,
	    "value":	123,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        print("============= step3 : test tags  ================")
        ### Default tag numeric types ###
        payload = ['''
        {
	    "metric":	"stb0_8",
	    "timestamp":	0,
	    "value":	123,
	    "tags":	{
		"t1":	123
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_8")
        tdSql.checkData(2, 1, "DOUBLE")

        payload = ['''
        {
	    "metric":	"stb0_9",
	    "timestamp":	0,
	    "value":	123,
	    "tags":	{
		"t1":	123.00
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_9")
        tdSql.checkData(2, 1, "DOUBLE")

        payload = ['''
        {
	    "metric":	"stb0_10",
	    "timestamp":	0,
	    "value":	123,
	    "tags":	{
		"t1":	123E-1
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb0_10")
        tdSql.checkData(2, 1, "DOUBLE")

        ### Nested format ###
        print("============= step4 : test nested format  ================")
        ### timestamp ###
        #seconds
        payload = ['''
        {
	    "metric":	"stb1_0",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	10,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select ts from stb1_0")
        tdSql.checkData(0, 0, "2021-07-11 20:33:53.000000")

        #milliseconds
        payload = ['''
        {
	    "metric":	"stb1_1",
	    "timestamp":	{
		"value":	1626006833610,
		"type":	"ms"
	    },
	    "value":	10,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select ts from stb1_1")
        tdSql.checkData(0, 0, "2021-07-11 20:33:53.610000")

        #microseconds
        payload = ['''
        {
	    "metric":	"stb1_2",
	    "timestamp":	{
		"value":	1626006833610123,
		"type":	"us"
	    },
	    "value":	10,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select ts from stb1_2")
        tdSql.checkData(0, 0, "2021-07-11 20:33:53.610123")

        #nanoseconds
        payload = ['''
        {
	    "metric":	"stb1_3",
	    "timestamp":	{
                "value":	1626006833610123321,
		"type":	"ns"
	    },
	    "value":	10,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("select ts from stb1_3")
        tdSql.checkData(0, 0, "2021-07-11 20:33:53.610123")

        #now
        tdSql.execute('use test')
        payload = ['''
        {
	    "metric":	"stb1_4",
	    "timestamp":	{
		"value":	0,
		"type":	"ns"
	    },
	    "value":	10,
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        ### metric value ###
        payload = ['''
        {
	    "metric":	"stb2_0",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	true,
		"type":	"bool"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_0")
        tdSql.checkData(1, 1, "BOOL")

        payload = ['''
        {
	    "metric":	"stb2_1",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	127,
		"type":	"tinyint"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_1")
        tdSql.checkData(1, 1, "TINYINT")

        payload = ['''
        {
	    "metric":	"stb2_2",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	32767,
		"type":	"smallint"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_2")
        tdSql.checkData(1, 1, "SMALLINT")

        payload = ['''
        {
	    "metric":	"stb2_3",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	2147483647,
		"type":	"int"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_3")
        tdSql.checkData(1, 1, "INT")

        payload = ['''
        {
	    "metric":	"stb2_4",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	9.2233720368547758e+18,
		"type":	"bigint"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_4")
        tdSql.checkData(1, 1, "BIGINT")

        payload = ['''
        {
	    "metric":	"stb2_5",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	11.12345,
		"type":	"float"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_5")
        tdSql.checkData(1, 1, "FLOAT")

        payload = ['''
        {
	    "metric":	"stb2_6",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	22.123456789,
		"type":	"double"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_6")
        tdSql.checkData(1, 1, "DOUBLE")

        payload = ['''
        {
	    "metric":	"stb2_7",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>",
		"type":	"binary"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_7")
        tdSql.checkData(1, 1, "BINARY")

        payload = ['''
        {
	    "metric":	"stb2_8",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	"你好",
		"type":	"nchar"
	    },
	    "tags":	{
		"t1":	true,
		"t2":	false,
		"t3":	10,
		"t4":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb2_8")
        tdSql.checkData(1, 1, "NCHAR")

        ### tag value ###

        payload = ['''
        {
	    "metric":	"stb3_0",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	"hello",
		"type":	"nchar"
	    },
	    "tags":	{
		"t1":	{
			"value":	true,
			"type":	"bool"
		},
		"t2":	{
			"value":	127,
			"type":	"tinyint"
		},
		"t3":	{
			"value":	32767,
			"type":	"smallint"
		},
		"t4":	{
			"value":	2147483647,
			"type":	"int"
		},
		"t5":	{
			"value":	9.2233720368547758e+18,
			"type":	"bigint"
		},
		"t6":	{
			"value":	11.12345,
			"type":	"float"
		},
		"t7":	{
			"value":	22.123456789,
			"type":	"double"
		},
		"t8":	{
			"value":	"binary_val",
			"type":	"binary"
		},
		"t9":	{
			"value":	"你好",
			"type":	"nchar"
		}
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe stb3_0")
        tdSql.checkData(2, 1, "BOOL")
        tdSql.checkData(3, 1, "TINYINT")
        tdSql.checkData(4, 1, "SMALLINT")
        tdSql.checkData(5, 1, "INT")
        tdSql.checkData(6, 1, "BIGINT")
        tdSql.checkData(7, 1, "FLOAT")
        tdSql.checkData(8, 1, "DOUBLE")
        tdSql.checkData(9, 1, "BINARY")
        tdSql.checkData(10, 1, "NCHAR")

        ### special characters ###

        payload = ['''
        {
	    "metric":	 "1234",
	    "timestamp": 1626006833,
	    "value":     1,
	    "tags":	{
		"id":	        "123",
		"456":	        true,
		"int":	        false,
		"double":       1,
		"into":         1,
		"from":         2,
		"!@#$.%^&*()":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe `1234`")
        tdSql.checkRows(9)

        #tdSql.query("select * from `123`")
        #tdSql.checkRows(1)

        payload = ['''
        {
	    "metric":	 "int",
	    "timestamp": 1626006833,
	    "value":     1,
	    "tags":	{
		"id":	        "and",
		"456":	        true,
		"int":	        false,
		"double":       1,
		"into":         1,
		"from":         2,
		"!@#$.%^&*()":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe `int`")
        tdSql.checkRows(9)

        #tdSql.query("select * from `and`")
        #tdSql.checkRows(1)

        payload = ['''
        {
	    "metric":	 "double",
	    "timestamp": 1626006833,
	    "value":     1,
	    "tags":	{
		"id":	        "for",
		"456":	        true,
		"int":	        false,
		"double":       1,
		"into":         1,
		"from":         2,
		"!@#$.%^&*()":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe `double`")
        tdSql.checkRows(9)

        #tdSql.query("select * from `for`")
        #tdSql.checkRows(1)

        payload = ['''
        {
	    "metric":	 "from",
	    "timestamp": 1626006833,
	    "value":     1,
	    "tags":	{
		"id":	        "!@#.^&",
		"456":	        true,
		"int":	        false,
		"double":       1,
		"into":         1,
		"from":         2,
		"!@#$.%^&*()":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe `from`")
        tdSql.checkRows(9)

        #tdSql.query("select * from `!@#.^&`")
        #tdSql.checkRows(1)

        payload = ['''
        {
	    "metric":	 "!@#$.%^&*()",
	    "timestamp": 1626006833,
	    "value":     1,
	    "tags":	{
		"id":	        "none",
		"456":	        true,
		"int":	        false,
		"double":       1,
		"into":         1,
		"from":         2,
		"!@#$.%^&*()":	"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>"
	    }
        }
        ''']
        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe `!@#$.%^&*()`")
        tdSql.checkRows(9)

        #tdSql.query("select * from `none`")
        #tdSql.checkRows(1)

        payload = ['''
        {
	    "metric":	"STABLE",
	    "timestamp":	{
		"value":	1626006833,
		"type":	"s"
	    },
	    "value":	{
		"value":	"hello",
		"type":	"nchar"
	    },
	    "tags":	{
                "id":   "KEY",
		"456":	{
			"value":	true,
			"type":	"bool"
		},
		"int":	{
			"value":	127,
			"type":	"tinyint"
		},
		"double":{
			"value":	32767,
			"type":	"smallint"
		},
		"into":	{
			"value":	2147483647,
			"type":	"int"
		},
		"INSERT":	{
			"value":	9.2233720368547758e+18,
			"type":	"bigint"
		},
		"!@#$.%^&*()":	{
			"value":	11.12345,
			"type":	"float"
		}
	    }
        }
        ''']

        code = self._conn.schemaless_insert(payload, TDSmlProtocolType.JSON.value, TDSmlTimestampType.NOT_CONFIGURED.value)
        print("schemaless_insert result {}".format(code))

        tdSql.query("describe `STABLE`")
        tdSql.checkRows(9)

        #tdSql.query("select * from `key`")
        #tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
