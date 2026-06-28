###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db.stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool
import copy
import json
import math
import os


class TestTaosdumpDataTypes:
    def _datadir(self, subdir):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", subdir)

    def _prepare_dir(self, path):
        if os.path.exists(path):
            os.system("rm -rf %s" % path)
        os.makedirs(path)


    #
    # ------------------- test_taosdump_test_type_big_int.py ----------------
    #
    def do_taosdump_type_big_int(self, mode = ""):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(4)

            tdSql.query("select * from db.st where bntag = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 1)

            tdSql.query("select * from db.st where bntag = 9223372036854775807")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 9223372036854775807)
            tdSql.checkData(0, 2, 9223372036854775807)

            tdSql.query("select * from db.st where bntag = -9223372036854775807")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, -9223372036854775807)
            tdSql.checkData(0, 2, -9223372036854775807)

            tdSql.query("select * from db.st where bntag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type big int ....................... [passed]")

    #
    # ------------------- test_taosdump_test_type_binary.py ----------------
    #
    def do_taosdump_type_binary(self, mode):

        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")            
            tdSql.execute("drop database if exists db")
            print(f"%s -i %s" % (tool, self.tmpdir))
            os.system(f"%s -i %s" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(2)
            dbresult = tdSql.queryResult
            print(dbresult)
            for i in range(len(dbresult)):
                assert dbresult[i][0] in ("t1", "t2")

            tdSql.query("select distinct(btag) from db.st where tbname = 't1'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "test")

            tdSql.query("select distinct(btag) from db.st where tbname = 't2'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, None)

            tdSql.query("select * from db.st where btag = 'test'")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, "01234")
            tdSql.checkData(0, 2, "56789")
            tdSql.checkData(1, 1, "abcd")
            tdSql.checkData(1, 2, "efgh")

            tdSql.query("select * from db.st where btag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type binary ........................ [passed]")

    def do_taosdump_type_decimal(self, mode):
        if mode == "-Z 'WebSocket'":
            tdLog.debug("WebSocket mode does not support decimal type, skip this test")
            return


        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult
            assert any(row[0] == "db" for row in dbresult)

            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(2)
            dbresult = tdSql.queryResult
            print(dbresult)
            for i in range(len(dbresult)):
                assert dbresult[i][0] in ("t1", "t2")

            tdSql.query("select distinct(dtag) from db.st where tbname = 't1'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 98765.123456)

            tdSql.query("select distinct(dtag) from db.st where tbname = 't2'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, None)

            tdSql.query("select * from db.st where dtag = 98765.123456")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, "98765432109876.1234567890123456")
            tdSql.checkData(0, 2, "56789.1234567890")

            tdSql.query("select * from db.st where dtag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type decimal ........................ [passed]")


    def do_taosdump_type_blob(self, mode):
        if mode == "-Z 'WebSocket'":
            tdLog.debug("WebSocket mode does not support blob type, skip this test")
            return


        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult
            assert any(row[0] == "db" for row in dbresult)

            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(2)
            dbresult = tdSql.queryResult
            for i in range(len(dbresult)):
                assert dbresult[i][0] in ("t1", "t2")

            # verify tag
            tdSql.query("select distinct(ntag) from db.st where tbname = 't1'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)

            tdSql.query("select distinct(ntag) from db.st where tbname = 't2'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, None)

            # row with simple strings
            tdSql.query("select * from db.st where ntag = 1 order by ts limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, b"abc")

            # row with embedded 0-byte
            tdSql.query("select c1 from db.st where ntag = 1 order by ts limit 1 offset 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, b"\x61\x62\x00\x63")

            # null row
            tdSql.query("select * from db.st where ntag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            print("do type blob .......................... [passed]")


    #
    # ------------------- test_taosdump_test_type_bool.py ----------------
    #
    def do_taosdump_type_bool(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(3)
            dbresult = tdSql.queryResult
            print(dbresult)
            for i in range(len(dbresult)):
                assert (
                    (dbresult[i][0] == "t1")
                    or (dbresult[i][0] == "t2")
                    or (dbresult[i][0] == "t3")
                )

            tdSql.query("select btag from db.st")
            tdSql.checkRows(3)
            dbresult = tdSql.queryResult
            print(dbresult)

            tdSql.query("select * from  db.st where btag = true")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, "True")
            tdSql.checkData(0, 2, "True")

            tdSql.query("select * from  db.st where btag = false")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, "False")
            tdSql.checkData(0, 2, "False")

            tdSql.query("select * from  db.st where btag is null")
            dbresult = tdSql.queryResult
            print(dbresult)
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type bool ....................... [passed]")

    #
    # ------------------- test_taosdump_test_type_double.py ----------------
    #
    def do_taosdump_type_double(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(4)

            tdSql.query("select * from db.st where dbtag = 1.0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            if not math.isclose(tdSql.getData(0, 1), 1.0):
                tdLog.debug("getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), 1.0))
                tdLog.exit("data is different")
            if not math.isclose(tdSql.getData(0, 2), 1.0):
                tdLog.debug("getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), 1.0))
                tdLog.exit("data is different")

            tdSql.query("select * from db.st where dbtag = 1.7E308")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            if not math.isclose(tdSql.getData(0, 1), 1.7e308):
                tdLog.debug(
                    "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), 1.7e308)
                )
                tdLog.exit("data is different")
            if not math.isclose(tdSql.getData(0, 2), 1.7e308):
                tdLog.debug(
                    "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), 1.7e308)
                )
                tdLog.exit("data is different")

            tdSql.query("select * from db.st where dbtag = -1.7E308")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            if not math.isclose(tdSql.getData(0, 1), -1.7e308):
                tdLog.debug(
                    "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), -1.7e308)
                )
                tdLog.exit("data is different")
            if not math.isclose(tdSql.getData(0, 2), -1.7e308):
                tdLog.debug(
                    "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), -1.7e308)
                )
                tdLog.exit("data is different")

            tdSql.query("select * from db.st where dbtag is null")
            dbresult = tdSql.queryResult
            print(dbresult)

            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type double ....................... [passed]")

    #
    # ------------------- test_taosdump_test_type_float.py ----------------
    #
    def do_taosdump_type_float(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(4)

            tdSql.query("select * from db.st where ftag = 1.0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            if not math.isclose(tdSql.getData(0, 1), 1.0):
                tdLog.debug("getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), 1.0))
                tdLog.exit("data is different")
            if not math.isclose(tdSql.getData(0, 2), 1.0):
                tdLog.exit("data is different")

            tdSql.query("select * from db.st where ftag > 3.399999E38 and ftag < 3.4000001E38")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            if not math.isclose(tdSql.getData(0, 1), 3.4e38, rel_tol=1e-07, abs_tol=0.0):
                tdLog.debug(
                    "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), 3.4e38)
                )
                tdLog.exit("data is different")
            if not math.isclose(tdSql.getData(0, 2), 3.4e38, rel_tol=1e-07, abs_tol=0.0):
                tdLog.debug(
                    "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), 3.4e38)
                )
                tdLog.exit("data is different")

            tdSql.query("select * from db.st where ftag < -3.399999E38 and ftag > -3.4000001E38")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            if not math.isclose(tdSql.getData(0, 1), (-3.4e38), rel_tol=1e-07, abs_tol=0.0):
                tdLog.debug(
                    "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), -3.4e38)
                )
                tdLog.exit("data is different")
            if not math.isclose(tdSql.getData(0, 2), (-3.4e38), rel_tol=1e-07, abs_tol=0.0):
                tdLog.debug(
                    "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), -3.4e38)
                )
                tdLog.exit("data is different")

            tdSql.query("select * from db.st where ftag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            print("do type float ......................... [passed]")

    #
    # ------------------- test_taosdump_test_type_int.py ----------------
    #
    def do_taosdump_type_int(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(4)

            tdSql.query("select * from db.st where ntag = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 1)

            tdSql.query("select * from db.st where ntag = 2147483647")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 2147483647)
            tdSql.checkData(0, 2, 2147483647)

            tdSql.query("select * from db.st where ntag = -2147483647")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, -2147483647)
            tdSql.checkData(0, 2, -2147483647)

            tdSql.query("select * from db.st where ntag is null")
            dbresult = tdSql.queryResult
            print(dbresult)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type int ........................... [passed]")

    #
    # ------------------- test_taosdump_test_type_json.py ----------------
    #
    def do_taosdump_type_json(self, mode):


        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(3)

            dbresult = tdSql.queryResult
            print(dbresult)
            for i in range(len(dbresult)):
                assert (
                    (dbresult[i][0] == "t1")
                    or (dbresult[i][0] == "t2")
                    or (dbresult[i][0] == "t3")
                )

            tdSql.query("select jtag->'location' from db.st")
            tdSql.checkRows(3)

            dbresult = tdSql.queryResult
            print(dbresult)
            found = False
            for i in range(len(dbresult)):
                if dbresult[i][0] == '"beijing"':
                    found = True
                    break

            assert found == True

            tdSql.query("select * from db.st where jtag contains 'location'")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)
            val = '{"location":"beijing"}'
            tdSql.checkData(0, 2, val)

            tdSql.query("select jtag from db.st")
            tdSql.checkRows(3)

            dbresult = tdSql.queryResult
            print(dbresult)
            found = False
            for i in range(len(dbresult)):
                if dbresult[i][0] == val:
                    found = True
                    break

            assert found == True

            print("do type json .......................... [passed]")

    #
    # ------------------- test_taosdump_test_type_small_int.py ----------------
    #
    def do_taosdump_type_small_int(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(4)

            tdSql.query("select * from db.st where sntag = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 1)

            tdSql.query("select * from db.st where sntag = 32767")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 32767)
            tdSql.checkData(0, 2, 32767)

            tdSql.query("select * from db.st where sntag = -32767")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, -32767)
            tdSql.checkData(0, 2, -32767)

            tdSql.query("select * from db.st where sntag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type small int ..................... [passed]")

    #
    # ------------------- test_taosdump_test_type_tiny_int.py ----------------
    #
    def do_taosdump_type_tiny_int(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(4)

            tdSql.query("select * from db.st where tntag = 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 1)

            tdSql.query("select * from db.st where tntag = 127")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 127)
            tdSql.checkData(0, 2, 127)

            tdSql.query("select * from db.st where tntag = -127")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, -127)
            tdSql.checkData(0, 2, -127)

            tdSql.query("select * from db.st where tntag is null")
            dbresult = tdSql.queryResult
            print(dbresult)

            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type tiny int ...................... [passed]")

    #
    # ------------------- test_taosdump_test_type_unsigned_big_int.py ----------------
    #
    def do_taosdump_type_unsigned_big_int(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(3)

            tdSql.query("select * from db.st where ubntag = 0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, 0)

            tdSql.query("select * from db.st where ubntag = 18446744073709551614")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 18446744073709551614)
            tdSql.checkData(0, 2, 18446744073709551614)

            tdSql.query("select * from db.st where ubntag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type unsigned big int .............. [passed]")

    #
    # ------------------- test_taosdump_test_type_unsigned_int.py ----------------
    #
    def do_taosdump_type_unsigned_int(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(3)

            tdSql.query("select * from db.st where untag = 0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, 0)

            tdSql.query("select * from db.st where untag = 4294967294")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 4294967294)
            tdSql.checkData(0, 2, 4294967294)

            tdSql.query("select * from db.st where untag is null")
            dbresult = tdSql.queryResult
            print(dbresult)

            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type unsigned int .................. [passed]")

    #
    # ------------------- test_taosdump_test_type_unsigned_small_int.py ----------------
    #
    def do_taosdump_type_unsigned_small_int(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(3)

            tdSql.query("select * from db.st where usntag = 0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, 0)

            tdSql.query("select * from db.st where usntag = 65534")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 65534)
            tdSql.checkData(0, 2, 65534)

            tdSql.query("select * from db.st where usntag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)

            print("do type unsigned small int ............ [passed]")

    #
    # ------------------- test_taosdump_test_type_unsigned_tiny_int.py ----------------
    #
    def do_taosdump_type_unsigned_tiny_int(self, mode):



        for tool_name, tool in [("taosBackup", self.newTaosdump)]:
            tdLog.info(f"--- {tool_name} import+verify ---")
            tdSql.execute("drop database if exists db")
            os.system(f"%s -i %s -T 1" % (tool, self.tmpdir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "db":
                    found = True
                    break

            assert found == True


            tdSql.query("show db.stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show db.tables")
            tdSql.checkRows(3)

            tdSql.query("select * from db.st where utntag = 0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, 0)

            tdSql.query("select * from db.st where utntag = 254")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, 254)
            tdSql.checkData(0, 2, 254)

            tdSql.query("select * from db.st where utntag is null")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1640000000000)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            print("do type unsigned tiny int ............. [passed]")

    #
    # ------------------- test_taosdump_type_geometry.py ----------------
    #
    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def findPrograme(self):
        # taosdump
        taosdump = etool.taosOldDumpFile()
        if taosdump == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % taosdump)

        # taosBenchmark
        benchmark = etool.benchMarkFile()
        if benchmark == "":
            tdLog.exit("benchmark not found!")
        else:
            tdLog.info("benchmark found in %s" % benchmark)

        # tmp dir
        tmpdir = "./tmp"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s/*" % tmpdir)

        return taosdump, benchmark,tmpdir

    def checkCorrectWithJson(self, jsonFile, newdb = None, checkInterval=False):
        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)

        # db come from arguments
        if newdb is None:
            db = data["databases"][0]["dbinfo"]["name"]
        else:
            db = newdb

        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(f"get json: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")

        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        if checkInterval:
            sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
            tdSql.query(sql)
            tdSql.checkRows(0)

    def testBenchmarkJson(self, benchmark, jsonFile, options="", checkInterval=False):
        # exe insert
        cmd = f"{benchmark} {options} -f {jsonFile}"
        self.exec(cmd)
        self.checkCorrectWithJson(jsonFile)

    def dumpOut(self, taosdump, db , outdir):
        # dump out
        self.exec(f"{taosdump} -D {db} -o {outdir}")

    def dumpIn(self, taosdump, db, newdb, indir):
        # dump in
        self.exec(f'{taosdump} -W "{db}={newdb}" -i {indir}')

    def checkAggSame(self, db, newdb, stb, aggfun):
        # sum pk db
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        sum1 = tdSql.getData(0,0)
        # sum pk newdb
        sql = f"select {aggfun} from {newdb}.{stb}"
        tdSql.query(sql)
        sum2 = tdSql.getData(0,0)

        if sum1 == sum2:
            tdLog.info(f"{aggfun} source db:{sum1} import db:{sum2} both equal.")
        else:
            tdLog.exit(f"{aggfun} source db:{sum1} import db:{sum2} not equal.")

    def checkProjSame(self, db, newdb, stb , row, col, where = "where tbname='d0'"):
        # sum pk db
        sql = f"select * from {db}.{stb} {where} limit {row+1}"
        tdSql.query(sql)
        val1 = copy.deepcopy(tdSql.getData(row, col))
        # sum pk newdb
        sql = f"select * from {newdb}.{stb} {where} limit {row+1}"
        tdSql.query(sql)
        val2 = copy.deepcopy(tdSql.getData(row, col))

        if val1 == val2:
            tdLog.info(f"{db}.{stb} {row},{col} source db:{val1} import db:{val2} both equal.")
        else:
            len1 = len(val1) if isinstance(val1, (str, bytes)) else 'N/A'
            len2 = len(val2) if isinstance(val2, (str, bytes)) else 'N/A'
            tdLog.exit(f"{db}.{stb} {row},{col} source db:{val1} len={len1} import db:{val2} len={len2} not equal, sql:{sql}.")

    def insertDataGeometry(self, benchmark, json, db):
        # insert super table
        self.testBenchmarkJson(benchmark, json)

        # normal table
        sqls = [
            f"create table {db}.ntb(st timestamp, c1 int, c2 geometry(128))",
            f"insert into {db}.ntb values(now, 0,  NULL)",
            f"insert into {db}.ntb values(now, 1, 'POINT(2 5)')",
            f"insert into {db}.ntb values(now, 2, 'LINESTRING(2 5, 4 7)')",
            f"insert into {db}.ntb values(now, 3, 'LINESTRING(2 6, 4 8, 9 3)')",
            f"insert into {db}.ntb values(now, 4, 'LINESTRING(2 5, 4 9)')",
            f"insert into {db}.ntb values(now, 5, 'LINESTRING(2 9, 3 14,6 19)')"
        ]
        for sql in sqls:
            tdSql.execute(sql)

    def verifyResultGeometry(self, db, newdb, json):
        # compare with insert json
        self.checkCorrectWithJson(json, newdb)

        #  compare sum(pk)
        stb = "meters"
        self.checkAggSame(db, newdb, stb, "sum(ic)")
        self.checkAggSame(db, newdb, stb, "sum(usi)")
        self.checkProjSame(db, newdb, stb, 0, 3)
        self.checkProjSame(db, newdb, stb, 0, 4)
        self.checkProjSame(db, newdb, stb, 0, 6) # tag

        self.checkProjSame(db, newdb, stb, 8, 3)
        self.checkProjSame(db, newdb, stb, 8, 4)
        self.checkProjSame(db, newdb, stb, 8, 6) # tag


        # check normal table
        self.checkAggSame(db, newdb, "ntb", "sum(c1)")
        # 0 line
        self.checkProjSame(db, newdb, "ntb", 0, 0, "")
        self.checkProjSame(db, newdb, "ntb", 0, 1, "")
        self.checkProjSame(db, newdb, "ntb", 0, 2, "")
        # 3 line
        self.checkProjSame(db, newdb, "ntb", 3, 0, "")
        self.checkProjSame(db, newdb, "ntb", 3, 1, "")
        self.checkProjSame(db, newdb, "ntb", 3, 2, "")

    def do_taosdump_type_geometry(self):
        # database
        db = "geodb"
        newdb = "ngeodb"
        datadir = self._datadir("dt_geometry")
        json_file = os.path.dirname(__file__) + "/json/geometry.json"
        taosbackup = etool.taosDumpFile()

        # import and verify with taosBackup
        tdSql.execute(f"drop database if exists {newdb}")
        self.exec(f'{taosbackup} -W "{db}={newdb}" -i {datadir}')

        # verify newdb directly (source db not available in compatibility test)
        self.checkCorrectWithJson(json_file, newdb)
        stb = "meters"
        tdSql.query(f"select sum(ic) from {newdb}.{stb}")
        tdSql.checkRows(1)
        tdSql.query(f"select sum(usi) from {newdb}.{stb}")
        tdSql.checkRows(1)
        # normal table: 6 rows inserted
        tdSql.query(f"select count(*) from {newdb}.ntb")
        tdSql.checkData(0, 0, 6)

        tdSql.execute(f"drop database if exists {newdb}")
        print("do type geometry ...................... [passed]")

    #
    # ------------------- test_taosdump_type_varbinary.py ----------------
    #
    def insertDataVarbinary(self, benchmark, json, db):
        # insert super table
        self.testBenchmarkJson(benchmark, json)

        # normal table
        sqls = [
            f"create table {db}.ntb(st timestamp, c1 int, c2 varbinary(32))",
            f"insert into {db}.ntb values(now, 0, 'abc1')",
            f"insert into {db}.ntb values(now, 1,  NULL)",
            f"insert into {db}.ntb values(now, 2, '\\x616263')",
            f"insert into {db}.ntb values(now, 3, 'abc3')",
            f"insert into {db}.ntb values(now, 4, 'abc4')",
            f"insert into {db}.ntb values(now, 5, 'abc5')",
        ]
        for sql in sqls:
            tdSql.execute(sql)

    def verifyResultVarbinary(self, db, newdb, json):
        # compare with insert json
        self.checkCorrectWithJson(json, newdb)

        #  compare sum(pk)
        stb = "meters"
        self.checkAggSame(db, newdb, stb, "sum(ic)")
        self.checkAggSame(db, newdb, stb, "sum(usi)")
        self.checkProjSame(db, newdb, stb, 0, 3)
        self.checkProjSame(db, newdb, stb, 0, 4)
        self.checkProjSame(db, newdb, stb, 0, 6) # tag

        self.checkProjSame(db, newdb, stb, 8, 3)
        self.checkProjSame(db, newdb, stb, 8, 4)
        self.checkProjSame(db, newdb, stb, 8, 6) # tag

        # check normal table
        tb = "ntb"
        self.checkAggSame(db, newdb, tb, "sum(c1)")
        # 0 line
        self.checkProjSame(db, newdb, tb, 0, 0, "")
        self.checkProjSame(db, newdb, tb, 0, 1, "")
        self.checkProjSame(db, newdb, tb, 0, 2, "")
        # 1 line
        self.checkProjSame(db, newdb, tb, 1, 0, "")
        self.checkProjSame(db, newdb, tb, 1, 1, "")
        self.checkProjSame(db, newdb, tb, 1, 2, "")
        # 3 line
        self.checkProjSame(db, newdb, tb, 3, 0, "")
        self.checkProjSame(db, newdb, tb, 3, 1, "")
        self.checkProjSame(db, newdb, tb, 3, 2, "")

    def do_taosdump_type_varbinary(self):
        # database
        db = "varbin"
        newdb = "nvarbin"
        datadir = self._datadir("dt_varbinary")
        json_file = os.path.dirname(__file__) + "/json/varbinary.json"
        taosbackup = etool.taosDumpFile()

        # import and verify with taosBackup
        tdSql.execute(f"drop database if exists {newdb}")
        self.exec(f'{taosbackup} -W "{db}={newdb}" -i {datadir}')

        # verify newdb directly (source db not available in compatibility test)
        self.checkCorrectWithJson(json_file, newdb)
        stb = "meters"
        tdSql.query(f"select sum(ic) from {newdb}.{stb}")
        tdSql.checkRows(1)
        tdSql.query(f"select sum(usi) from {newdb}.{stb}")
        tdSql.checkRows(1)
        # normal table: 6 rows inserted
        tdSql.query(f"select count(*) from {newdb}.ntb")
        tdSql.checkData(0, 0, 6)

        tdSql.execute(f"drop database if exists {newdb}")
        print("do type varbinary ..................... [passed]")


    #
    # ------------------- main ----------------
    #
    def do_all_datatypes(self, mode):
        type_dirs = [
            ("big_int", self.do_taosdump_type_big_int),
            ("binary", self.do_taosdump_type_binary),
            ("bool", self.do_taosdump_type_bool),
            ("double", self.do_taosdump_type_double),
            ("float", self.do_taosdump_type_float),
            ("json", self.do_taosdump_type_json),
            ("small_int", self.do_taosdump_type_small_int),
            ("int", self.do_taosdump_type_int),
            ("tiny_int", self.do_taosdump_type_tiny_int),
            ("unsigned_big_int", self.do_taosdump_type_unsigned_big_int),
            ("unsigned_int", self.do_taosdump_type_unsigned_int),
            ("unsigned_small_int", self.do_taosdump_type_unsigned_small_int),
            ("unsigned_tiny_int", self.do_taosdump_type_unsigned_tiny_int),
            ("decimal", self.do_taosdump_type_decimal),
            ("blob", self.do_taosdump_type_blob),
        ]
        for type_name, method in type_dirs:
            self.tmpdir = self._datadir(f"dt_{type_name}")
            if not os.path.exists(self.tmpdir):
                os.makedirs(self.tmpdir)
            method(mode)

    def test_taosdump_datatypes(self):
        """taosdump data types

        1. taosdump type big int
        2. taosdump type binary
        3. taosdump type bool
        4. taosdump type double
        5. taosdump type float
        6. taosdump type json
        7. taosdump type small int
        8. taosdump type int
        9. taosdump type tiny int
        10. taosdump type unsigned big int
        11. taosdump type unsigned int
        12. taosdump type unsigned small int
        13. taosdump type unsigned tiny int
        14. taosdump type geometry
        15. taosdump type varbinary

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_big_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_binary.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_bool.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_double.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_float.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_json.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_small_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_tiny_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_unsigned_big_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_unsigned_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_unsigned_small_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_type_unsigned_tiny_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_type_geometry.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_type_varbinary.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_big_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_binary.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_bool.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_double.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_float.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_json.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_small_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_tiny_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_unsigned_big_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_unsigned_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_unsigned_small_int.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_test_type_unsigned_tiny_int.py
        """
        # init
        self.newTaosdump = etool.taosDumpFile()
        self.tmpdir = self._datadir("dt_tmp")  # will be overridden per type

        self.do_all_datatypes("-Z 'Native'")
        self.do_taosdump_type_geometry()
        self.do_taosdump_type_varbinary()
