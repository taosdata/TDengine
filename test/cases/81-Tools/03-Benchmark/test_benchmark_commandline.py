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
import os
import platform
import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, etool, sc


class TestBenchmarkCommandline:
    #
    # ------------------- test_commandline_partial_col_numpy.py ----------------
    #
    def do_commandline_partial_col_numpy(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -t 1 -n 1 -y -L 2 " % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        tdSql.query("select * from test.meters")
        dbresult = tdSql.queryResult
        for i in range(len(dbresult[0])):
            if i in (1, 2) and dbresult[0][i] is None:
                tdLog.exit("result[0][%d] is NULL, which should not be" % i)
            else:
                tdLog.info("result[0][{0}] is {1}".format(i, dbresult[0][i]))

        tdSql.checkData(0, 0, 1500000000000)
        tdSql.checkData(0, 3, None)

        print("\n")
        print("do command line partial columns ....... [passed]")

    #
    # ------------------- test_commandline_retry.py ----------------
    #
    def do_commandline_retry(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -t 1 -n 10 -i 1000 -r 1 -k 10 -z 1000 -y &" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(2)
        sc.dnodeStopAll()
        time.sleep(2)
        sc.dnodeStart(1)
        time.sleep(2)
        
        sql = "select count(*) from test.meters"
        tdSql.checkDataLoop(0, 0, 10, sql)

        print("do command line retry ................. [passed]")
    
    #
    # ------------------- test_commandline_single_table.py ----------------
    #    
    def do_commandline_single_table(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -N -I taosc -t 1 -n 1 -y -E" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from `meters`")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -N -I rest -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -N -I stmt -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1)
        
        print("do command line single table .......... [passed]")

    #
    # ------------------- test_commandline_sml.py ----------------
    #
    def do_commandline_sml(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -I sml -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-line -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-telnet -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-json -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-taosjson -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml -t 10 -n 10000  -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 10*10000)

        # add normal table
        cmd = "-N -I sml -t 2 -n 10000  -y"
        rlist = self.benchmark(cmd, checkRun = False)
        # expect failed
        self.checkListString(rlist, "schemaless cannot work without stable")

        print("do command line sml protocol .......... [passed]")

    #
    # ------------------- test_commandline_supplement_insert.py ----------------
    #
    def do_commandline_supplement_insert(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        cmd = "%s -t 1 -n 10 -U -s 1600000000000 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 11)
        tdSql.query("select * from test.meters")
        tdSql.checkData(0, 0, 1500000000000)
        tdSql.checkData(1, 0, 1600000000000)
        tdSql.checkData(10, 0, 1600000000009)

        print("do command line insert ................ [passed]")

    #
    # ------------------- test_commandline_vgroups.py ----------------
    #
    def do_commandline_vgroups(self):
        binPath = etool.benchMarkFile()
        cmd = (
            "%s -t 1 -n 1 -v 3 -y &"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(2)

        tdSql.query("select `vgroups` from information_schema.ins_databases where name='test'")
        tdSql.checkData(0, 0, 3)
        print("do command line vgroups ............... [passed]")

    #
    # ------------------- test_commandline.py ----------------
    #
    def checkVersion(self):
        # run
        outputs = etool.runBinFile("taosBenchmark", "-V")
        print(outputs)
        if len(outputs) != 4:
            tdLog.exit(f"checkVersion return lines count {len(outputs)} != 4")
        # version string len
        assert len(outputs[1]) > 24
        # commit id
        assert len(outputs[2]) > 43
        assert outputs[2][:4] == "git:"
        # build info
        assert len(outputs[3]) > 36
        assert outputs[3][:6] == "build:"

        tdLog.info("check taosBenchmark version successfully.")        


    def do_commandline(self):
        # check version
        self.checkVersion()

        # command line
        binPath = etool.benchMarkFile()
        if platform.system().lower() == "windows":
            cmd = (
            "%s -F 7 -n 10 -t 2 -x -y -M -C -d newtest -l 5 -A binary,nchar(31) -b tinyint,binary(23),bool,nchar -w 29 -E -m $%%^*"
            % binPath
            )
        else:
            # For Linux and MacOS
            cmd = (
                "%s -F 7 -n 10 -t 2 -x -y -M -C -d newtest -l 5 -A binary,nchar\(31\) -b tinyint,binary\(23\),bool,nchar -w 29 -E -m $%%^*"
                % binPath
            )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use newtest")
        tdSql.query("select count(*) from newtest.meters")
        tdSql.checkData(0, 0, 20)
        tdSql.query("describe meters")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "TINYINT")
        # 2.x is binary and 3.x is varchar
        # tdSql.checkData(2, 1, "BINARY")
        tdSql.checkData(2, 2, 23)
        tdSql.checkData(3, 1, "BOOL")
        tdSql.checkData(4, 1, "NCHAR")
        tdSql.checkData(4, 2, 29)
        tdSql.checkData(5, 1, "INT")
        # 2.x is binary and 3.x is varchar
        # tdSql.checkData(6, 1, "BINARY")
        tdSql.checkData(6, 2, 29)
        tdSql.checkData(6, 3, "TAG")
        tdSql.checkData(7, 1, "NCHAR")
        tdSql.checkData(7, 2, 31)
        tdSql.checkData(7, 3, "TAG")
        tdSql.query("show tables")
        tdSql.checkRows(2)
        tdSql.execute("drop database if exists newtest")

        cmd = (
            "%s -t 2 -n 10 -b bool,tinyint,smallint,int,bigint,float,double,utinyint,usmallint,uint,ubigint,binary,nchar,timestamp,varbinary,geometry -A bool,tinyint,smallint,int,bigint,float,double,utinyint,usmallint,uint,ubigint,binary,nchar,timestamp,varbinary,geometry -y"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("show test.tables")
        tdSql.checkRows(2)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        cmd = (
            "%s -I stmt -t 2 -n 10 -b bool,tinyint,smallint,int,bigint,float,double,utinyint,usmallint,uint,ubigint,binary,nchar,timestamp,varbinary,geometry -A bool,tinyint,smallint,int,bigint,float,double,utinyint,usmallint,uint,ubigint,binary,nchar,timestamp,varbinary,geometry -y"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("show test.tables")
        tdSql.checkRows(2)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        cmd = "%s -F 7 -n 10 -t 2 -y -M -I stmt" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("show test.tables")
        tdSql.checkRows(2)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        # add stmt2
        cmd = "%s -F 700 -n 1000 -t 4 -y -M -I stmt2" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("show test.tables")
        tdSql.checkRows(4)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 4000)

        cmd = "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 2>&1 | grep sleep | wc -l" % binPath
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 2:
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 2>&1 | grep sleep | wc -l" % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 3:
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -I sml 2>&1 | grep sleep | wc -l"
            % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 2:
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 -I sml 2>&1 | grep sleep | wc -l"
            % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 3:
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -I stmt 2>&1 | grep sleep | wc -l"
            % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 2:
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 -I stmt 2>&1 | grep sleep | wc -l"
            % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 3:
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = "%s -S 17 -n 3 -t 1 -y -x" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(2)  # to avoid invalid vgroup id
        tdSql.query("select last(ts) from test.meters")
        tdSql.checkData(0, 0, "2017-07-14 10:40:00.034")

        cmd = "%s -N -I taosc -t 11 -n 11 -y -x -E -c abcde" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from `d10`")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -N -I rest -t 11 -n 11 -y -x -c /etc/taos" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from d10")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -N -I stmt -t 11 -n 11 -y -x" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from d10")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -n 1 -t 1 -y -b bool" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BOOL")

        cmd = "%s -n 1 -t 1 -y -b tinyint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TINYINT")

        cmd = "%s -n 1 -t 1 -y -b utinyint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TINYINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b smallint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "SMALLINT")

        cmd = "%s -n 1 -t 1 -y -b usmallint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "SMALLINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b int" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "INT")

        cmd = "%s -n 1 -t 1 -y -b uint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "INT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b bigint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BIGINT")

        cmd = "%s -n 1 -t 1 -y -b ubigint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BIGINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b timestamp" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TIMESTAMP")

        cmd = "%s -n 1 -t 1 -y -b float" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "FLOAT")

        cmd = "%s -n 1 -t 1 -y -b double" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "DOUBLE")

        cmd = "%s -n 1 -t 1 -y -b nchar" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "NCHAR")

        cmd = "%s -n 1 -t 1 -y -b nchar\(7\)" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "NCHAR")

        cmd = "%s -n 1 -t 1 -y -A json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(4, 1, "JSON")

        cmd = "%s -f ./81-Tools/03-Benchmark/json/insert-sample.json -j ./insert_json_res.json" % binPath
        tdLog.info("%s" % cmd)
        ret = os.system("%s" % cmd)
        if not os.path.exists("./insert_json_res.json"):
            tdLog.info(f"expect failed, JSON file does not exist, cmd={cmd} ret={ret}")

        cmd = "%s -n 1 -t 1 -y -b int,x" % binPath
        ret = os.system("%s" % cmd)
        if ret == 0:
            tdLog.exit(f"expect failed, but successful cmd= {cmd} ")
        tdLog.info(f"test except ok, cmd={cmd} ret={ret}")

        print("do command line ....................... [passed]")

    #
    # ------------------- test_invalid_commandline.py ----------------
    #
    def do_invalid_commandline(self):

        binPath = etool.benchMarkFile()
        cmd = (
            "%s -F abc -P abc -I abc -T abc -i abc -S abc -B abc -r abc -t abc -n abc -l abc -w abc -w 16385 -R abc -O abc -a abc -n 2 -t 2 -r 1 -y"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 4)

        cmd = "%s non_exist_opt" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        cmd = "%s -f non_exist_file -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        cmd = "%s -h non_exist_host -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        cmd = "%s -p non_exist_pass -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        cmd = "%s -u non_exist_user -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        print("do invalid command line ............... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_commandline(self): 
        """taosBenchmark command line

        1. Verify -c with different config path
        2. Verify -I with taosc, rest, stmt, stmt2, sml, sml-line, sml-telnet, sml-json, sml-taosjson
        3. Verify -b with all data types
        4. Verify -v with support vgroups
        5. Verify -A with all data types
        6. Verify -S to set start time
        7. Verify -U to do supplement insert
        8. Verify -V to check version info
        9. Verify other command line options -ntyNxGrTiFM
        10. Verify JIRA TD-11510/TD-19387/TD-19985/TD-21063/TD-22334/TD-21932/TD-19352/TD-21806
        11. Verify invalid command line options
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:            
            - 2025-10-24 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline_partial_col_numpy.py
            - 2025-10-24 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline_retry.py
            - 2025-10-24 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline_single_table.py 
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline_sml.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline_supplement_insert.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline_vgroups.py
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_invalid_commandline.py

        """
        self.do_commandline()
        self.do_commandline_partial_col_numpy()
        self.do_commandline_single_table()
        self.do_commandline_sml()
        self.do_commandline_supplement_insert()
        self.do_commandline_vgroups()
        self.do_commandline_retry()
        self.do_invalid_commandline()