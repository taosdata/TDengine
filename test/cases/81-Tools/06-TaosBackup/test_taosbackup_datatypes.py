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
        self.dumpIn(taosbackup, db, newdb, tmpdir)
        self.verifyResultVarbinary(db, newdb, jsonFile)

        tdLog.info("do_taosbackup_type_varbinary ................. [passed]")

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
        All types tested with both Native and WebSocket connections.

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
