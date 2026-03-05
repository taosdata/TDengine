###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
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


class TestTaosBackupDataTypes:

    #
    # ------------------- big int ----------------
    #
    def do_taosbackup_type_big_int(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute("create table db.st(ts timestamp, c1 BIGINT) tags(bntag BIGINT)")
        tdSql.execute("create table db.t1 using db.st tags(1)")
        tdSql.execute("insert into db.t1 values(1640000000000, 1)")
        tdSql.execute("create table db.t2 using db.st tags(9223372036854775807)")
        tdSql.execute("insert into db.t2 values(1640000000000, 9223372036854775807)")
        tdSql.execute("create table db.t3 using db.st tags(-9223372036854775807)")
        tdSql.execute("insert into db.t3 values(1640000000000, -9223372036854775807)")
        tdSql.execute("create table db.t4 using db.st tags(NULL)")
        tdSql.execute("insert into db.t4 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(4)

        tdSql.query("select * from db.st where bntag = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from db.st where bntag = 9223372036854775807")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 9223372036854775807)
        tdSql.checkData(0, 2, 9223372036854775807)

        tdSql.query("select * from db.st where bntag = -9223372036854775807")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -9223372036854775807)
        tdSql.checkData(0, 2, -9223372036854775807)

        tdSql.query("select * from db.st where bntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_big_int ................... [passed]")

    #
    # ------------------- binary ----------------
    #
    def do_taosbackup_type_binary(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute(
            "create table db.st(ts timestamp, c1 BINARY(5), c2 BINARY(5)) tags(btag BINARY(5))"
        )
        tdSql.execute("create table db.t1 using db.st tags('test')")
        tdSql.execute("insert into db.t1 values(1640000000000, '01234', '56789')")
        tdSql.execute("insert into db.t1 values(1640000000001, 'abcd', 'efgh')")
        tdSql.execute("create table db.t2 using db.st tags(NULL)")
        tdSql.execute("insert into db.t2 values(1640000000000, NULL, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(2)

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

        tdLog.info("do_taosbackup_type_binary .................... [passed]")

    #
    # ------------------- bool ----------------
    #
    def do_taosbackup_type_bool(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute("create table db.st(ts timestamp, c1 BOOL) tags(btag BOOL)")
        tdSql.execute("create table db.t1 using db.st tags(true)")
        tdSql.execute("insert into db.t1 values(1640000000000, true)")
        tdSql.execute("create table db.t2 using db.st tags(false)")
        tdSql.execute("insert into db.t2 values(1640000000000, false)")
        tdSql.execute("create table db.t3 using db.st tags(NULL)")
        tdSql.execute("insert into db.t3 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(3)

        tdSql.query("select * from db.st where btag = true")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "True")
        tdSql.checkData(0, 2, "True")

        tdSql.query("select * from db.st where btag = false")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "False")
        tdSql.checkData(0, 2, "False")

        tdSql.query("select * from db.st where btag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_bool ...................... [passed]")

    #
    # ------------------- double ----------------
    #
    def do_taosbackup_type_double(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute("create table db.st(ts timestamp, c1 DOUBLE) tags(dbtag DOUBLE)")
        tdSql.execute("create table db.t1 using db.st tags(1.0)")
        tdSql.execute("insert into db.t1 values(1640000000000, 1.0)")
        tdSql.execute("create table db.t2 using db.st tags(1.7E308)")
        tdSql.execute("insert into db.t2 values(1640000000000, 1.7E308)")
        tdSql.execute("create table db.t3 using db.st tags(-1.7E308)")
        tdSql.execute("insert into db.t3 values(1640000000000, -1.7E308)")
        tdSql.execute("create table db.t4 using db.st tags(NULL)")
        tdSql.execute("insert into db.t4 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(4)

        tdSql.query("select * from db.st where dbtag = 1.0")
        tdSql.checkRows(1)
        assert math.isclose(tdSql.getData(0, 1), 1.0), "double 1.0 mismatch"
        assert math.isclose(tdSql.getData(0, 2), 1.0), "double tag 1.0 mismatch"

        tdSql.query("select * from db.st where dbtag = 1.7E308")
        tdSql.checkRows(1)
        assert math.isclose(tdSql.getData(0, 1), 1.7e308), "double 1.7E308 mismatch"

        tdSql.query("select * from db.st where dbtag = -1.7E308")
        tdSql.checkRows(1)
        assert math.isclose(tdSql.getData(0, 1), -1.7e308), "double -1.7E308 mismatch"

        tdSql.query("select * from db.st where dbtag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_double .................... [passed]")

    #
    # ------------------- float ----------------
    #
    def do_taosbackup_type_float(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute("create table db.st(ts timestamp, c1 FLOAT) tags(ftag FLOAT)")
        tdSql.execute("create table db.t1 using db.st tags(1.0)")
        tdSql.execute("insert into db.t1 values(1640000000000, 1.0)")
        tdSql.execute("create table db.t2 using db.st tags(3.40E+38)")
        tdSql.execute("insert into db.t2 values(1640000000000, 3.40E+38)")
        tdSql.execute("create table db.t3 using db.st tags(-3.40E+38)")
        tdSql.execute("insert into db.t3 values(1640000000000, -3.40E+38)")
        tdSql.execute("create table db.t4 using db.st tags(NULL)")
        tdSql.execute("insert into db.t4 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(4)

        tdSql.query("select * from db.st where ftag = 1.0")
        tdSql.checkRows(1)
        assert math.isclose(tdSql.getData(0, 1), 1.0), "float 1.0 mismatch"

        tdSql.query(
            "select * from db.st where ftag > 3.399999E38 and ftag < 3.4000001E38"
        )
        tdSql.checkRows(1)
        assert math.isclose(
            tdSql.getData(0, 1), 3.4e38, rel_tol=1e-07
        ), "float 3.4E38 mismatch"

        tdSql.query(
            "select * from db.st where ftag < -3.399999E38 and ftag > -3.4000001E38"
        )
        tdSql.checkRows(1)
        assert math.isclose(
            tdSql.getData(0, 1), -3.4e38, rel_tol=1e-07
        ), "float -3.4E38 mismatch"

        tdSql.query("select * from db.st where ftag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_float ..................... [passed]")

    #
    # ------------------- int ----------------
    #
    def do_taosbackup_type_int(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute("create table db.st(ts timestamp, c1 INT) tags(ntag INT)")
        tdSql.execute("create table db.t1 using db.st tags(1)")
        tdSql.execute("insert into db.t1 values(1640000000000, 1)")
        tdSql.execute("create table db.t2 using db.st tags(2147483647)")
        tdSql.execute("insert into db.t2 values(1640000000000, 2147483647)")
        tdSql.execute("create table db.t3 using db.st tags(-2147483647)")
        tdSql.execute("insert into db.t3 values(1640000000000, -2147483647)")
        tdSql.execute("create table db.t4 using db.st tags(NULL)")
        tdSql.execute("insert into db.t4 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(4)

        tdSql.query("select * from db.st where ntag = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from db.st where ntag = 2147483647")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2147483647)
        tdSql.checkData(0, 2, 2147483647)

        tdSql.query("select * from db.st where ntag = -2147483647")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -2147483647)
        tdSql.checkData(0, 2, -2147483647)

        tdSql.query("select * from db.st where ntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_int ....................... [passed]")

    #
    # ------------------- json ----------------
    #
    def do_taosbackup_type_json(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute("create table db.st(ts timestamp, c1 int) tags(jtag JSON)")
        tdSql.execute('create table db.t1 using db.st tags(\'{"location": "beijing"}\')')
        tdSql.execute("insert into db.t1 values(1500000000000, 1)")
        tdSql.execute("create table db.t2 using db.st tags(NULL)")
        tdSql.execute("insert into db.t2 values(1500000000000, NULL)")
        tdSql.execute("create table db.t3 using db.st tags('')")
        tdSql.execute("insert into db.t3 values(1500000000000, 0)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(3)

        tdSql.query("select jtag->'location' from db.st")
        tdSql.checkRows(3)
        found = any(row[0] == '"beijing"' for row in tdSql.queryResult)
        assert found, "'beijing' JSON value not found"

        tdSql.query("select * from db.st where jtag contains 'location'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        val = '{"location":"beijing"}'
        tdSql.checkData(0, 2, val)

        tdLog.info("do_taosbackup_type_json ...................... [passed]")

    #
    # ------------------- smallint ----------------
    #
    def do_taosbackup_type_small_int(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute(
            "create table db.st(ts timestamp, c1 SMALLINT) tags(sntag SMALLINT)"
        )
        tdSql.execute("create table db.t1 using db.st tags(1)")
        tdSql.execute("insert into db.t1 values(1640000000000, 1)")
        tdSql.execute("create table db.t2 using db.st tags(32767)")
        tdSql.execute("insert into db.t2 values(1640000000000, 32767)")
        tdSql.execute("create table db.t3 using db.st tags(-32767)")
        tdSql.execute("insert into db.t3 values(1640000000000, -32767)")
        tdSql.execute("create table db.t4 using db.st tags(NULL)")
        tdSql.execute("insert into db.t4 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.tables")
        tdSql.checkRows(4)

        tdSql.query("select * from db.st where sntag = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from db.st where sntag = 32767")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 32767)
        tdSql.checkData(0, 2, 32767)

        tdSql.query("select * from db.st where sntag = -32767")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -32767)
        tdSql.checkData(0, 2, -32767)

        tdSql.query("select * from db.st where sntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_small_int ................. [passed]")

    #
    # ------------------- tinyint ----------------
    #
    def do_taosbackup_type_tiny_int(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute(
            "create table db.st(ts timestamp, c1 TINYINT) tags(tntag TINYINT)"
        )
        tdSql.execute("create table db.t1 using db.st tags(1)")
        tdSql.execute("insert into db.t1 values(1640000000000, 1)")
        tdSql.execute("create table db.t2 using db.st tags(127)")
        tdSql.execute("insert into db.t2 values(1640000000000, 127)")
        tdSql.execute("create table db.t3 using db.st tags(-127)")
        tdSql.execute("insert into db.t3 values(1640000000000, -127)")
        tdSql.execute("create table db.t4 using db.st tags(NULL)")
        tdSql.execute("insert into db.t4 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.tables")
        tdSql.checkRows(4)

        tdSql.query("select * from db.st where tntag = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from db.st where tntag = 127")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 127)
        tdSql.checkData(0, 2, 127)

        tdSql.query("select * from db.st where tntag = -127")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -127)
        tdSql.checkData(0, 2, -127)

        tdSql.query("select * from db.st where tntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_tiny_int .................. [passed]")

    #
    # ------------------- unsigned bigint ----------------
    #
    def do_taosbackup_type_unsigned_big_int(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute(
            "create table db.st(ts timestamp, c1 BIGINT UNSIGNED) tags(ubntag BIGINT UNSIGNED)"
        )
        tdSql.execute("create table db.t1 using db.st tags(0)")
        tdSql.execute("insert into db.t1 values(1640000000000, 0)")
        tdSql.execute("create table db.t2 using db.st tags(18446744073709551614)")
        tdSql.execute("insert into db.t2 values(1640000000000, 18446744073709551614)")
        tdSql.execute("create table db.t3 using db.st tags(NULL)")
        tdSql.execute("insert into db.t3 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.tables")
        tdSql.checkRows(3)

        tdSql.query("select * from db.st where ubntag = 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 0)

        tdSql.query("select * from db.st where ubntag = 18446744073709551614")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 18446744073709551614)
        tdSql.checkData(0, 2, 18446744073709551614)

        tdSql.query("select * from db.st where ubntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_unsigned_big_int .......... [passed]")

    #
    # ------------------- unsigned int ----------------
    #
    def do_taosbackup_type_unsigned_int(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute(
            "create table db.st(ts timestamp, c1 INT UNSIGNED) tags(untag INT UNSIGNED)"
        )
        tdSql.execute("create table db.t1 using db.st tags(0)")
        tdSql.execute("insert into db.t1 values(1640000000000, 0)")
        tdSql.execute("create table db.t2 using db.st tags(4294967294)")
        tdSql.execute("insert into db.t2 values(1640000000000, 4294967294)")
        tdSql.execute("create table db.t3 using db.st tags(NULL)")
        tdSql.execute("insert into db.t3 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.tables")
        tdSql.checkRows(3)

        tdSql.query("select * from db.st where untag = 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 0)

        tdSql.query("select * from db.st where untag = 4294967294")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 4294967294)
        tdSql.checkData(0, 2, 4294967294)

        tdSql.query("select * from db.st where untag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_unsigned_int .............. [passed]")

    #
    # ------------------- unsigned smallint ----------------
    #
    def do_taosbackup_type_unsigned_small_int(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute(
            "create table db.st(ts timestamp, c1 SMALLINT UNSIGNED) tags(usntag SMALLINT UNSIGNED)"
        )
        tdSql.execute("create table db.t1 using db.st tags(0)")
        tdSql.execute("insert into db.t1 values(1640000000000, 0)")
        tdSql.execute("create table db.t2 using db.st tags(65534)")
        tdSql.execute("insert into db.t2 values(1640000000000, 65534)")
        tdSql.execute("create table db.t3 using db.st tags(NULL)")
        tdSql.execute("insert into db.t3 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.tables")
        tdSql.checkRows(3)

        tdSql.query("select * from db.st where usntag = 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 0)

        tdSql.query("select * from db.st where usntag = 65534")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 65534)
        tdSql.checkData(0, 2, 65534)

        tdSql.query("select * from db.st where usntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_unsigned_small_int ........ [passed]")

    #
    # ------------------- unsigned tinyint ----------------
    #
    def do_taosbackup_type_unsigned_tiny_int(self, mode=""):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")

        tdSql.execute(
            "create table db.st(ts timestamp, c1 TINYINT UNSIGNED) tags(utntag TINYINT UNSIGNED)"
        )
        tdSql.execute("create table db.t1 using db.st tags(0)")
        tdSql.execute("insert into db.t1 values(1640000000000, 0)")
        tdSql.execute("create table db.t2 using db.st tags(254)")
        tdSql.execute("insert into db.t2 values(1640000000000, 254)")
        tdSql.execute("create table db.t3 using db.st tags(NULL)")
        tdSql.execute("insert into db.t3 values(1640000000000, NULL)")

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system(f"%s {mode} -D db -o %s -T 1" % (self.binPath, self.tmpdir))
        tdSql.execute("drop database db")
        os.system(f"%s {mode} -i %s -T 1" % (self.binPath, self.tmpdir))

        tdSql.query("show databases")
        assert any(row[0] == "db" for row in tdSql.queryResult), "db not found"

        tdSql.query("show db.tables")
        tdSql.checkRows(3)

        tdSql.query("select * from db.st where utntag = 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 0)

        tdSql.query("select * from db.st where utntag = 254")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 254)
        tdSql.checkData(0, 2, 254)

        tdSql.query("select * from db.st where utntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.info("do_taosbackup_type_unsigned_tiny_int ......... [passed]")

    #
    # ------------------- geometry ----------------
    #
    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def findPrograme(self):
        taosbackup = etool.taosBackupFile()
        if taosbackup == "":
            tdLog.exit("taosBackup not found!")

        benchmark = etool.benchMarkFile()
        if benchmark == "":
            tdLog.exit("benchmark not found!")

        tmpdir = "./tmp"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            os.system("rm -rf %s/*" % tmpdir)

        return taosbackup, benchmark, tmpdir

    def checkCorrectWithJson(self, jsonFile, newdb=None, checkInterval=False):
        with open(jsonFile, "r") as f:
            data = json.load(f)
        db = newdb if newdb else data["databases"][0]["dbinfo"]["name"]
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(
            f"check json: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows}"
        )
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        if checkInterval:
            sql = (
                f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) "
                f"where dif != {timestamp_step};"
            )
            tdSql.query(sql)
            tdSql.checkRows(0)

    def dumpOut(self, taosbackup, db, outdir):
        self.exec(f"{taosbackup} -D {db} -o {outdir}")

    def dumpIn(self, taosbackup, db, newdb, indir):
        self.exec(f'{taosbackup} -W "{db}={newdb}" -i {indir}')

    def checkAggSame(self, db, newdb, stb, aggfun):
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        sum1 = tdSql.getData(0, 0)
        sql = f"select {aggfun} from {newdb}.{stb}"
        tdSql.query(sql)
        sum2 = tdSql.getData(0, 0)
        if sum1 == sum2:
            tdLog.info(f"{aggfun} source:{sum1} import:{sum2} equal.")
        else:
            tdLog.exit(f"{aggfun} source:{sum1} import:{sum2} not equal.")

    def checkProjSame(self, db, newdb, stb, row, col, where="where tbname='d0'"):
        sql = f"select * from {db}.{stb} {where} limit {row+1}"
        tdSql.query(sql)
        val1 = copy.deepcopy(tdSql.getData(row, col))
        sql = f"select * from {newdb}.{stb} {where} limit {row+1}"
        tdSql.query(sql)
        val2 = copy.deepcopy(tdSql.getData(row, col))
        if val1 == val2:
            tdLog.info(f"{stb}[{row},{col}] source:{val1} import:{val2} equal.")
        else:
            len1 = len(val1) if isinstance(val1, (str, bytes)) else "N/A"
            len2 = len(val2) if isinstance(val2, (str, bytes)) else "N/A"
            tdLog.exit(
                f"{stb}[{row},{col}] source:{val1} len={len1} import:{val2} len={len2} not equal."
            )

    def insertDataGeometry(self, benchmark, jsonFile, db):
        self.exec(f"{benchmark} -f {jsonFile}")
        self.checkCorrectWithJson(jsonFile)

        sqls = [
            f"create table {db}.ntb(st timestamp, c1 int, c2 geometry(128))",
            f"insert into {db}.ntb values(now, 0, NULL)",
            f"insert into {db}.ntb values(now, 1, 'POINT(2 5)')",
            f"insert into {db}.ntb values(now, 2, 'LINESTRING(2 5, 4 7)')",
            f"insert into {db}.ntb values(now, 3, 'LINESTRING(2 6, 4 8, 9 3)')",
            f"insert into {db}.ntb values(now, 4, 'LINESTRING(2 5, 4 9)')",
            f"insert into {db}.ntb values(now, 5, 'LINESTRING(2 9, 3 14,6 19)')",
        ]
        for sql in sqls:
            tdSql.execute(sql)

    def verifyResultGeometry(self, db, newdb, jsonFile):
        self.checkCorrectWithJson(jsonFile, newdb)
        stb = "meters"
        self.checkAggSame(db, newdb, stb, "sum(ic)")
        self.checkAggSame(db, newdb, stb, "sum(usi)")
        self.checkProjSame(db, newdb, stb, 0, 3)
        self.checkProjSame(db, newdb, stb, 0, 4)
        self.checkProjSame(db, newdb, stb, 0, 6)
        self.checkProjSame(db, newdb, stb, 8, 3)
        self.checkProjSame(db, newdb, stb, 8, 4)
        self.checkProjSame(db, newdb, stb, 8, 6)

        self.checkAggSame(db, newdb, "ntb", "sum(c1)")
        self.checkProjSame(db, newdb, "ntb", 0, 0, "")
        self.checkProjSame(db, newdb, "ntb", 0, 1, "")
        self.checkProjSame(db, newdb, "ntb", 0, 2, "")
        self.checkProjSame(db, newdb, "ntb", 3, 0, "")
        self.checkProjSame(db, newdb, "ntb", 3, 1, "")
        self.checkProjSame(db, newdb, "ntb", 3, 2, "")

    def do_taosbackup_type_geometry(self):
        db = "geodb"
        newdb = "ngeodb"

        taosbackup, benchmark, tmpdir = self.findPrograme()
        jsonFile = os.path.dirname(__file__) + "/json/geometry.json"

        self.insertDataGeometry(benchmark, jsonFile, db)
        self.dumpOut(taosbackup, db, tmpdir)
        # Drop restored DB so we start fresh and avoid stale tag values or
        # a leftover restore-checkpoint causing skipped data files.
        tdSql.execute(f"drop database if exists {newdb}")
        cpFile = os.path.join(tmpdir, db, "restore_checkpoint.txt")
        if os.path.exists(cpFile):
            os.remove(cpFile)
        self.dumpIn(taosbackup, db, newdb, tmpdir)
        self.verifyResultGeometry(db, newdb, jsonFile)

        tdLog.info("do_taosbackup_type_geometry .................. [passed]")

    #
    # ------------------- varbinary ----------------
    #
    def insertDataVarbinary(self, benchmark, jsonFile, db):
        self.exec(f"{benchmark} -f {jsonFile}")
        self.checkCorrectWithJson(jsonFile)

        sqls = [
            f"create table {db}.ntb(st timestamp, c1 int, c2 varbinary(32))",
            f"insert into {db}.ntb values(now, 0, 'abc1')",
            f"insert into {db}.ntb values(now, 1, NULL)",
            f"insert into {db}.ntb values(now, 2, '\\x616263')",
            f"insert into {db}.ntb values(now, 3, 'abc3')",
            f"insert into {db}.ntb values(now, 4, 'abc4')",
            f"insert into {db}.ntb values(now, 5, 'abc5')",
        ]
        for sql in sqls:
            tdSql.execute(sql)

    def verifyResultVarbinary(self, db, newdb, jsonFile):
        self.checkCorrectWithJson(jsonFile, newdb)
        stb = "meters"
        self.checkAggSame(db, newdb, stb, "sum(ic)")
        self.checkAggSame(db, newdb, stb, "sum(usi)")
        self.checkProjSame(db, newdb, stb, 0, 3)
        self.checkProjSame(db, newdb, stb, 0, 4)
        self.checkProjSame(db, newdb, stb, 0, 6)
        self.checkProjSame(db, newdb, stb, 8, 3)
        self.checkProjSame(db, newdb, stb, 8, 4)
        self.checkProjSame(db, newdb, stb, 8, 6)

        tb = "ntb"
        self.checkAggSame(db, newdb, tb, "sum(c1)")
        for row in [0, 1, 3]:
            for col in [0, 1, 2]:
                self.checkProjSame(db, newdb, tb, row, col, "")

    def do_taosbackup_type_varbinary(self):
        db = "varbin"
        newdb = "nvarbin"

        taosbackup, benchmark, tmpdir = self.findPrograme()
        jsonFile = os.path.dirname(__file__) + "/json/varbinary.json"

        self.insertDataVarbinary(benchmark, jsonFile, db)
        self.dumpOut(taosbackup, db, tmpdir)
        # Drop restored DB so we start fresh and avoid stale tag values or
        # a leftover restore-checkpoint causing skipped data files.
        tdSql.execute(f"drop database if exists {newdb}")
        cpFile = os.path.join(tmpdir, db, "restore_checkpoint.txt")
        if os.path.exists(cpFile):
            os.remove(cpFile)
        self.dumpIn(taosbackup, db, newdb, tmpdir)
        self.verifyResultVarbinary(db, newdb, jsonFile)

        tdLog.info("do_taosbackup_type_varbinary ................. [passed]")

    # ==========================================================================
    # do_taosbackup_all_types  –  two-layer architecture
    #
    #   Layer 1  do_taosbackup_all_types()     orchestration only
    #   Layer 2  at_*()                       concrete implementation
    # ==========================================================================

    # ------------------------------------------------------------------
    # Layer-2 helpers shared by setup and verify
    # ------------------------------------------------------------------

    # Column layout for STB / NTB (select * returns these in order):
    #   idx  0 = ts
    #   idx  1 = c_bool          BOOL
    #   idx  2 = c_tinyint       TINYINT
    #   idx  3 = c_smallint      SMALLINT
    #   idx  4 = c_int           INT
    #   idx  5 = c_bigint        BIGINT
    #   idx  6 = c_utinyint      TINYINT UNSIGNED
    #   idx  7 = c_usmallint     SMALLINT UNSIGNED
    #   idx  8 = c_uint          INT UNSIGNED
    #   idx  9 = c_ubigint       BIGINT UNSIGNED
    #   idx 10 = c_float         FLOAT
    #   idx 11 = c_double        DOUBLE
    #   idx 12 = c_binary        BINARY(20)
    #   idx 13 = c_nchar         NCHAR(20)
    #   idx 14 = c_varbinary     VARBINARY(32)
    #   idx 15 = c_geometry      GEOMETRY(128)
    #   idx 16 = c_decimal       DECIMAL(10,3)
    #   idx 17 = c_blob          BLOB
    #
    # Tag layout for STB (explicit select returns these in order):
    #   idx  0 = t_bool          BOOL
    #   idx  1 = t_tinyint       TINYINT
    #   idx  2 = t_smallint      SMALLINT
    #   idx  3 = t_int           INT
    #   idx  4 = t_bigint        BIGINT
    #   idx  5 = t_utinyint      TINYINT UNSIGNED
    #   idx  6 = t_usmallint     SMALLINT UNSIGNED
    #   idx  7 = t_uint          INT UNSIGNED
    #   idx  8 = t_ubigint       BIGINT UNSIGNED
    #   idx  9 = t_float         FLOAT
    #   idx 10 = t_double        DOUBLE
    #   idx 11 = t_binary        BINARY(20)
    #   idx 12 = t_nchar         NCHAR(20)
    #   idx 13 = t_varbinary     VARBINARY(20)
    #   idx 14 = t_geometry      GEOMETRY(64)
    # Note: DECIMAL is NOT a supported tag type in TDengine; tested as column only.

    # Non-NULL column values written into row ts=1640000000000
    _AT_COL_VALS = (
        "true,"                         # c_bool
        " 127,"                         # c_tinyint
        " 32767,"                       # c_smallint
        " 2147483647,"                  # c_int
        " 9223372036854775807,"         # c_bigint
        " 254,"                         # c_utinyint
        " 65534,"                       # c_usmallint
        " 4294967294,"                  # c_uint
        " 18446744073709551614,"        # c_ubigint
        " 3.14,"                        # c_float
        " 2.718281828459045,"           # c_double
        " 'hello',"                     # c_binary
        " '世界',"                      # c_nchar
        " '\\x48454c4c4f',"             # c_varbinary  (HELLO)
        " 'POINT(1.0 2.0)',"            # c_geometry
        " 123.456,"                     # c_decimal
        " 'blobdata'"                   # c_blob  (plain string — BLOB accepts varchar-style)
    )

    # Non-NULL tag values for ct1
    _AT_TAG_VALS = (
        "true,"                         # t_bool
        " 100,"                         # t_tinyint
        " 1000,"                        # t_smallint
        " 2000000,"                     # t_int
        " 9000000000,"                  # t_bigint
        " 200,"                         # t_utinyint
        " 60000,"                       # t_usmallint
        " 3000000000,"                  # t_uint
        " 10000000000,"                 # t_ubigint
        " 1.5,"                         # t_float
        " 2.718281828459045,"           # t_double
        " 'tag_bin',"                   # t_binary
        " '标签',"                      # t_nchar
        " '\\x414243',"                 # t_varbinary  (ABC)
        " 'POINT(3.0 4.0)'"             # t_geometry
    )

    # ------------------------------------------------------------------
    # Layer-2: setup
    # ------------------------------------------------------------------

    def at_create_tables(self, db):
        """Create DB, STB (all non-JSON col + tag types), CTBs and NTB."""
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} keep 3649")

        # Super table: every non-JSON column type + every non-JSON tag type
        tdSql.execute(
            f"create table {db}.st("
            "  ts           timestamp,"
            "  c_bool       bool,"
            "  c_tinyint    tinyint,"
            "  c_smallint   smallint,"
            "  c_int        int,"
            "  c_bigint     bigint,"
            "  c_utinyint   tinyint unsigned,"
            "  c_usmallint  smallint unsigned,"
            "  c_uint       int unsigned,"
            "  c_ubigint    bigint unsigned,"
            "  c_float      float,"
            "  c_double     double,"
            "  c_binary     binary(20),"
            "  c_nchar      nchar(20),"
            "  c_varbinary  varbinary(32),"
            "  c_geometry   geometry(128),"
            "  c_decimal    decimal(10,3),"
            "  c_blob       blob"
            ") tags("
            "  t_bool       bool,"
            "  t_tinyint    tinyint,"
            "  t_smallint   smallint,"
            "  t_int        int,"
            "  t_bigint     bigint,"
            "  t_utinyint   tinyint unsigned,"
            "  t_usmallint  smallint unsigned,"
            "  t_uint       int unsigned,"
            "  t_ubigint    bigint unsigned,"
            "  t_float      float,"
            "  t_double     double,"
            "  t_binary     binary(20),"
            "  t_nchar      nchar(20),"
            "  t_varbinary  varbinary(20),"
            "  t_geometry   geometry(64)"
            ")"
            # Note: DECIMAL is not a supported tag type; tested as column only.
        )

        # ct1: non-NULL tags
        tdSql.execute(
            f"create table {db}.ct1 using {db}.st tags({self._AT_TAG_VALS})"
        )
        # ct2: all-NULL tags (tests NULL tag backup/restore)
        tdSql.execute(
            f"create table {db}.ct2 using {db}.st tags("
            "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,"
            "NULL,NULL,NULL,NULL,NULL,NULL,NULL)"
        )

        # Normal table: same column set, no tags
        tdSql.execute(
            f"create table {db}.nt1("
            "  ts           timestamp,"
            "  c_bool       bool,"
            "  c_tinyint    tinyint,"
            "  c_smallint   smallint,"
            "  c_int        int,"
            "  c_bigint     bigint,"
            "  c_utinyint   tinyint unsigned,"
            "  c_usmallint  smallint unsigned,"
            "  c_uint       int unsigned,"
            "  c_ubigint    bigint unsigned,"
            "  c_float      float,"
            "  c_double     double,"
            "  c_binary     binary(20),"
            "  c_nchar      nchar(20),"
            "  c_varbinary  varbinary(32),"
            "  c_geometry   geometry(128),"
            "  c_decimal    decimal(10,3),"
            "  c_blob       blob"
            ")"
        )

    def at_insert_data(self, db):
        """Insert one all-value row (ts=1640000000000) and one all-NULL row
        (ts=1640000000001) into every table."""
        null_cols = "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL"
        for tbl in [f"{db}.ct1", f"{db}.ct2", f"{db}.nt1"]:
            tdSql.execute(
                f"insert into {tbl} values(1640000000000, {self._AT_COL_VALS})"
            )
            tdSql.execute(
                f"insert into {tbl} values(1640000000001, {null_cols})"
            )

    def at_run_backup_restore(self, db, tmpdir):
        """Backup database, drop it, then restore from the backup."""
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            os.system(f"rm -rf {tmpdir}")
            os.makedirs(tmpdir)
        binPath = self.binPath
        os.system(f"{binPath} -D {db} -o {tmpdir} -T 1")
        tdSql.execute(f"drop database {db}")
        os.system(f"{binPath} -i {tmpdir} -T 1")

    # ------------------------------------------------------------------
    # Layer-2: verify helpers (reused across CTB and NTB)
    # ------------------------------------------------------------------

    def at_verify_nonnull_row(self, db, table):
        """Verify the all-value row (ts=1640000000000) in *table*."""
        tdSql.query(
            f"select c_bool,c_tinyint,c_smallint,c_int,c_bigint,"
            f"       c_utinyint,c_usmallint,c_uint,c_ubigint,"
            f"       c_float,c_double,c_binary,c_nchar,"
            f"       c_varbinary,c_geometry,c_decimal,c_blob"
            f"  from {db}.{table} where ts=1640000000000"
        )
        tdSql.checkRows(1)
        r = tdSql.queryResult[0]
        #  0  c_bool
        assert r[0] in (True, 1),                       f"{table}.c_bool:    {r[0]}"
        #  1  c_tinyint
        assert r[1] == 127,                              f"{table}.c_tinyint: {r[1]}"
        #  2  c_smallint
        assert r[2] == 32767,                            f"{table}.c_smallint:{r[2]}"
        #  3  c_int
        assert r[3] == 2147483647,                      f"{table}.c_int:     {r[3]}"
        #  4  c_bigint
        assert r[4] == 9223372036854775807,             f"{table}.c_bigint:  {r[4]}"
        #  5  c_utinyint
        assert r[5] == 254,                              f"{table}.c_utinyint:{r[5]}"
        #  6  c_usmallint
        assert r[6] == 65534,                            f"{table}.c_usmallint:{r[6]}"
        #  7  c_uint
        assert r[7] == 4294967294,                      f"{table}.c_uint:    {r[7]}"
        #  8  c_ubigint
        assert r[8] == 18446744073709551614,            f"{table}.c_ubigint: {r[8]}"
        #  9  c_float
        assert math.isclose(float(r[9]),  3.14, rel_tol=1e-5),  f"{table}.c_float:   {r[9]}"
        # 10  c_double
        assert math.isclose(float(r[10]), 2.718281828459045, rel_tol=1e-10), f"{table}.c_double:{r[10]}"
        # 11  c_binary
        assert r[11] == "hello",                         f"{table}.c_binary:  {r[11]}"
        # 12  c_nchar
        assert r[12] == "世界",                          f"{table}.c_nchar:   {r[12]}"
        # 13  c_varbinary  — raw bytes or str; just verify non-NULL and length
        assert r[13] is not None,                        f"{table}.c_varbinary is NULL"
        assert len(r[13]) == 5,                          f"{table}.c_varbinary len:{len(r[13])}"
        # 14  c_geometry  — server returns WKB bytes for column data
        #     WKB POINT = 21 bytes: 1-byte order + 4-byte type + 8-byte x + 8-byte y
        assert r[14] is not None,                        f"{table}.c_geometry is NULL"
        geom14 = bytes(r[14]) if not isinstance(r[14], bytes) else r[14]
        assert len(geom14) == 21,                        f"{table}.c_geometry len:{len(geom14)}"
        assert geom14[1:5] == b'\x01\x00\x00\x00',      f"{table}.c_geometry not POINT WKB"
        # 15  c_decimal
        assert abs(float(str(r[15])) - 123.456) < 0.001, f"{table}.c_decimal:{r[15]}"
        # 16  c_blob  — verify roundtrip (non-NULL); exact byte content varies by driver
        assert r[16] is not None,                        f"{table}.c_blob is NULL"

    def at_verify_null_row(self, db, table):
        """Verify the all-NULL row (ts=1640000000001) in *table*."""
        tdSql.query(
            f"select c_bool,c_tinyint,c_smallint,c_int,c_bigint,"
            f"       c_utinyint,c_usmallint,c_uint,c_ubigint,"
            f"       c_float,c_double,c_binary,c_nchar,"
            f"       c_varbinary,c_geometry,c_decimal,c_blob"
            f"  from {db}.{table} where ts=1640000000001"
        )
        tdSql.checkRows(1)
        r = tdSql.queryResult[0]
        for i, col in enumerate([
            "c_bool","c_tinyint","c_smallint","c_int","c_bigint",
            "c_utinyint","c_usmallint","c_uint","c_ubigint",
            "c_float","c_double","c_binary","c_nchar",
            "c_varbinary","c_geometry","c_decimal","c_blob",
        ]):
            assert r[i] is None, f"{table}.{col} should be NULL, got {r[i]}"

    def at_verify_ct1_tags(self, db):
        """Verify ct1's non-NULL tag values after restore."""
        tdSql.query(
            f"select t_bool,t_tinyint,t_smallint,t_int,t_bigint,"
            f"       t_utinyint,t_usmallint,t_uint,t_ubigint,"
            f"       t_float,t_double,t_binary,t_nchar,"
            f"       t_varbinary,t_geometry"
            f"  from {db}.st where tbname='ct1' limit 1"
        )
        tdSql.checkRows(1)
        r = tdSql.queryResult[0]
        assert r[0]  in (True, 1),                       f"ct1.t_bool:     {r[0]}"
        assert r[1]  == 100,                             f"ct1.t_tinyint:  {r[1]}"
        assert r[2]  == 1000,                            f"ct1.t_smallint: {r[2]}"
        assert r[3]  == 2000000,                         f"ct1.t_int:      {r[3]}"
        assert r[4]  == 9000000000,                      f"ct1.t_bigint:   {r[4]}"
        assert r[5]  == 200,                             f"ct1.t_utinyint: {r[5]}"
        assert r[6]  == 60000,                           f"ct1.t_usmallint:{r[6]}"
        assert r[7]  == 3000000000,                      f"ct1.t_uint:     {r[7]}"
        assert r[8]  == 10000000000,                     f"ct1.t_ubigint:  {r[8]}"
        assert math.isclose(float(r[9]),  1.5, rel_tol=1e-5),               f"ct1.t_float:   {r[9]}"
        assert math.isclose(float(r[10]), 2.718281828459045, rel_tol=1e-5),  f"ct1.t_double: {r[10]}"
        assert r[11] == "tag_bin",                       f"ct1.t_binary:   {r[11]}"
        assert r[12] == "标签",                          f"ct1.t_nchar:    {r[12]}"
        assert r[13] is not None,                        f"ct1.t_varbinary is NULL"
        assert len(r[13]) == 3,                          f"ct1.t_varbinary len:{len(r[13])}"
        assert r[14] is not None,                        f"ct1.t_geometry is NULL"
        geom14 = bytes(r[14]) if not isinstance(r[14], bytes) else r[14]
        assert len(geom14) == 21,                        f"ct1.t_geometry WKB len:{len(geom14)}"
        assert geom14[1:5] == b'\x01\x00\x00\x00',      f"ct1.t_geometry not POINT WKB"

    def at_verify_ct2_tags(self, db):
        """Verify ct2's all-NULL tag values after restore."""
        tdSql.query(
            f"select t_bool, t_int, t_binary, t_varbinary, t_geometry"
            f"  from {db}.st where tbname='ct2' limit 1"
        )
        tdSql.checkRows(1)
        r = tdSql.queryResult[0]
        for i, tag in enumerate(
            ["t_bool", "t_int", "t_binary", "t_varbinary", "t_geometry"]
        ):
            assert r[i] is None, f"ct2.{tag} should be NULL, got {r[i]}"

    # ------------------------------------------------------------------
    # Layer-2: verify orchestration
    # ------------------------------------------------------------------

    def at_verify_all(self, db):
        """Verify row counts, data values and tag values for all tables."""
        # --- row counts ---
        for tbl in ["ct1", "ct2", "nt1"]:
            tdSql.query(f"select count(*) from {db}.{tbl}")
            tdSql.checkData(0, 0, 2)

        # --- column data (non-NULL and NULL rows) ---
        for tbl in ["ct1", "ct2", "nt1"]:
            self.at_verify_nonnull_row(db, tbl)
            self.at_verify_null_row(db, tbl)

        # --- tag values ---
        self.at_verify_ct1_tags(db)
        self.at_verify_ct2_tags(db)

    # ------------------------------------------------------------------
    # Layer-1: entry point
    # ------------------------------------------------------------------

    def do_taosbackup_all_types(self):
        """Backup/restore covering ALL TDengine non-JSON data types as both
        columns and tags, in child tables and normal tables.

        Layer-1 orchestration:
          1. at_create_tables  – build DB, STB (all col/tag types), CTBs, NTB
          2. at_insert_data    – insert value row + NULL row into every table
          3. at_run_backup_restore – taosBackup -D / drop / -i
          4. at_verify_all     – check counts, column values, tag values
        """
        db     = "db_all"
        tmpdir = "./taosbackuptest/tmpdir_all_types"

        self.at_create_tables(db)
        self.at_insert_data(db)
        self.at_run_backup_restore(db, tmpdir)
        self.at_verify_all(db)

        tdLog.info("do_taosbackup_all_types ..................... [passed]")

    #
    # ------------------- run all data types ----------------
    #
    def do_all_datatypes(self, mode=""):
        self.do_taosbackup_type_big_int(mode)
        self.do_taosbackup_type_binary(mode)
        self.do_taosbackup_type_bool(mode)
        self.do_taosbackup_type_double(mode)
        self.do_taosbackup_type_float(mode)
        self.do_taosbackup_type_json(mode)
        self.do_taosbackup_type_small_int(mode)
        self.do_taosbackup_type_int(mode)
        self.do_taosbackup_type_tiny_int(mode)
        self.do_taosbackup_type_unsigned_big_int(mode)
        self.do_taosbackup_type_unsigned_int(mode)
        self.do_taosbackup_type_unsigned_small_int(mode)
        self.do_taosbackup_type_unsigned_tiny_int(mode)

    #
    # ------------------- main test method ----------------
    #
    def test_taosbackup_datatypes(self):
        """taosBackup data types

        1.  taosBackup type big int (boundary values + NULL)
        2.  taosBackup type binary (string values + NULL)
        3.  taosBackup type bool (true/false/NULL)
        4.  taosBackup type double (boundary values + NULL)
        5.  taosBackup type float (boundary values + NULL)
        6.  taosBackup type json (JSON tag values)
        7.  taosBackup type small int (boundary values + NULL)
        8.  taosBackup type int (boundary values + NULL)
        9.  taosBackup type tiny int (boundary values + NULL)
        10. taosBackup type unsigned big int (boundary values + NULL)
        11. taosBackup type unsigned int (boundary values + NULL)
        12. taosBackup type unsigned small int (boundary values + NULL)
        13. taosBackup type unsigned tiny int (boundary values + NULL)
        14. taosBackup type geometry (geometry column types)
        15. taosBackup type varbinary (binary data types)
        16. taosBackup all extended types: VARBINARY, GEOMETRY, DECIMAL(p,s), BLOB
        All numeric/string types tested with both Native and WebSocket connections.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Migrated and adapted from 04-Taosdump/test_taosdump_datatypes.py

        """
        self.binPath = etool.taosBackupFile()
        if self.binPath == "":
            tdLog.exit("taosBackup not found!")
        else:
            tdLog.info("taosBackup found: %s" % self.binPath)

        self.tmpdir = "./taosbackuptest/tmpdir_types"

        # Native mode
        self.do_all_datatypes("-Z Native")
        self.do_taosbackup_type_geometry()
        self.do_taosbackup_type_varbinary()

        # WebSocket mode
        self.do_all_datatypes("-Z WebSocket -X http://127.0.0.1:6041")

        # Extended types: VARBINARY, GEOMETRY, DECIMAL, BLOB
        self.do_taosbackup_all_types()
