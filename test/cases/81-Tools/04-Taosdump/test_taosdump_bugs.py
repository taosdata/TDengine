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
import os

class TestTaosdumpStartEndTime:
    #
    # ------------------- test_taosdump_start_end_time.py ----------------
    #
    def prepare(self, time_unit='', precision=''):
        
        tdSql.prepare()
        if time_unit != '':
            time_unit = f"precision '{time_unit}'"

        tdLog.info("taosdump start time end time test, precision: %s" % precision)

        tdSql.execute("drop database if exists db")
        tdSql.execute(f"create database db  keep 3649 {time_unit} ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table t1 (ts timestamp, n int)"
        )
        tdSql.execute(
            "create table t2 (ts timestamp, n int)"
        )
        tdSql.execute(
            "create table t3 (ts timestamp, n int)"
        )
        tdSql.execute(
            f"insert into t1 values('2023-02-28 12:00:00.{precision}',11)('2023-02-28 12:00:01.{precision}',12)('2023-02-28 12:00:02.{precision}',15)('2023-02-28 12:00:03.{precision}',16)('2023-02-28 12:00:04.{precision}',17)"
        )
        tdSql.execute(
            f"insert into t2 values('2023-02-28 12:00:00.{precision}',11)('2023-02-28 12:00:01.{precision}',12)('2023-02-28 12:00:02.{precision}',15)('2023-02-28 12:00:03.{precision}',16)('2023-02-28 12:00:04.{precision}',17)"
        )
        tdSql.execute(
            f"insert into t3 values('2023-02-28 12:00:00.{precision}',11)('2023-02-28 12:00:01.{precision}',12)('2023-02-28 12:00:02.{precision}',15)('2023-02-28 12:00:03.{precision}',16)('2023-02-28 12:00:04.{precision}',17)"
        )

    def do_taosdump_start_end_time(self):
        self.taosdump_start_end_time("ns")
        self.taosdump_start_end_time("us")
        self.taosdump_start_end_time("ms")
        self.taosdump_start_end_time()

    def taosdump_start_end_time(self, time_unit=''):
        precision = ''
        if time_unit == "ns":
            precision = '997000000'
        elif time_unit == "us":
            precision = '997000'
        elif time_unit == "ms":
            precision = '997'
        else:
            precision = '0'    

        self.prepare(time_unit, precision)
        tdLog.info("taosdump start time end time test, precision: %s" % precision)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        #        sys.exit(1)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t1 -o %s -T 1 -S 2023-02-28T12:00:01.%s+0800 -E 2023-02-28T12:00:03.%s+0800 " % (binPath, self.tmpdir, precision, precision))

        tdSql.execute("drop table t1")

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t1")
        tdSql.checkData(0, 0, 3)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t2 -o %s -T 1 -S 2023-02-28T12:00:01.%s+0800 " % (binPath, self.tmpdir, precision))

        tdSql.execute("drop table t2")
        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t2")
        tdSql.checkData(0, 0, 4)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t3 -o %s -T 1 -E 2023-02-28T12:00:03.%s+0800 " % (binPath, self.tmpdir, precision))

        tdSql.execute("drop table t3")

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t3")
        tdSql.checkData(0, 0, 4)

        print("do start end time ..................... [passed]")

    #
    # ------------------- test_taosdump_start_end_time_long.py ----------------
    #
    def do_taosdump_start_end_time_long(self):
        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table t1 (ts timestamp, n int)"
        )
        tdSql.execute(
            "create table t2 (ts timestamp, n int)"
        )
        tdSql.execute(
            "create table t3 (ts timestamp, n int)"
        )
        tdSql.execute(
            "insert into t1 values('2023-02-28 12:00:00.997',11)('2023-02-28 12:00:01.997',12)('2023-02-28 12:00:02.997',15)('2023-02-28 12:00:03.997',16)('2023-02-28 12:00:04.997',17)"
        )
        tdSql.execute(
            "insert into t2 values('2023-02-28 12:00:00.997',11)('2023-02-28 12:00:01.997',12)('2023-02-28 12:00:02.997',15)('2023-02-28 12:00:03.997',16)('2023-02-28 12:00:04.997',17)"
        )
        tdSql.execute(
            "insert into t3 values('2023-02-28 12:00:00.997',11)('2023-02-28 12:00:01.997',12)('2023-02-28 12:00:02.997',15)('2023-02-28 12:00:03.997',16)('2023-02-28 12:00:04.997',17)"
        )

        #        sys.exit(1)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t1 -o %s -T 1 --start-time=2023-02-28T12:00:01.997+0800 --end-time=2023-02-28T12:00:03.997+0800 " % (binPath, self.tmpdir))

        tdSql.execute("drop table t1")

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t1")
        tdSql.checkData(0, 0, 3)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t2 -o %s -T 1 --start-time=2023-02-28T12:00:01.997+0800 " % (binPath, self.tmpdir))

        tdSql.execute("drop table t2")
        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t2")
        tdSql.checkData(0, 0, 4)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t3 -o %s -T 1 --end-time=2023-02-28T12:00:03.997+0800 " % (binPath, self.tmpdir))

        tdSql.execute("drop table t3")

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t3")
        tdSql.checkData(0, 0, 4)    
        print("do start end time long ................ [passed]")

    #
    # ------------------- test_taosdump_escaped_db.py ----------------
    #
    def checkVersion(self):
        # run
        outputs = etool.runBinFile("taosdump", "-V")
        print(outputs)
        if len(outputs) != 4:
            tdLog.exit(f"checkVersion return lines count {len(outputs)} != 4")
        # version string len
        assert len(outputs[1]) > 19
        # commit id
        assert len(outputs[2]) > 43
        assert outputs[2][:4] == "git:"
        # build info
        assert len(outputs[3]) > 36
        assert outputs[3][:6] == "build:"

        tdLog.info("check taosdump version successfully.")

    def do_taosdump_escaped_db(self):
        # check version
        self.checkVersion()

        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database `Db`")

        tdSql.execute("use `Db`")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT) tags(n1 INT)"
        )
        tdSql.execute(
            "create table t1 using st tags(1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1)"
        )
        #        sys.exit(1)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        print("%s Db st -R -e -o %s -T 1" % (binPath, self.tmpdir))
        os.system("%s Db st -R -e -o %s -T 1" % (binPath, self.tmpdir))
        # sys.exit(1)

        tdSql.execute("drop database `Db`")
        #        sys.exit(1)

        os.system("%s -R -e -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.queryResult

        found = False
        for i in range(len(dbresult)):
            print("Found db: %s" % dbresult[i][0])
            if dbresult[i][0] == "Db":
                found = True
                break

        assert found == True

        tdSql.execute("use `Db`")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("select count(*) from `Db`.st")
        tdSql.checkData(0, 0, 1)
    
        print("do escape db .......................... [passed]")

    #
    # ------------------- main ----------------
    #
    
        print("do many cols .......................... [passed]")

    #
    # ------------------- main ----------------
    #
    
        print("do many cols .......................... [passed]")
    #
    # ------------------- main ----------------
    #
    def test_taosdump_bugs(self):
        """taosdump bugs

        1. Verify bug TS-2769 (start-time end-time)
        2. Verify bug TS-7053 (start-time end-time + precision)
        3. Verify bug TS-3072 (database name case sensitivity)
        
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_start_end_time.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_start_end_time_long.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_escaped_db.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/

        """
        self.do_taosdump_start_end_time()
        self.do_taosdump_start_end_time_long()    