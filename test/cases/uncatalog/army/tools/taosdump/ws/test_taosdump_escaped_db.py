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

class TestTaosdumpEscapedDb:
    def caseDescription(self):
        """
        case1<sdsang>: [TS-3072] taosdump dump escaped db name test
        """

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


    def test_taosdump_escaped_db(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
            - xxx
        """
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

        tdLog.success("%s successfully executed" % __file__)


