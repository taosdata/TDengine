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
import json
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        case for test schema changed 
        """

    def createDir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            print("directory exists")
            self.clearPath(path)

    def clearPath(self, path):
        os.system("rm -rf %s/*" % path)


    # insert
    def insertData(self):
        # source db
        command = "-f tools/taosdump/native/json/schemaChange.json"
        self.benchmark(command)
        # des newdb
        command = "-f tools/taosdump/native/json/schemaChangeNew.json"
        self.benchmark(command)

    # dump out
    def dumpOut(self, db, tmpdir):
        cmd = f"-D {db} -o {tmpdir}"
        rlist = self.taosdump(cmd)
        results = [
            "OK: total 10 table(s) of stable: meters1 schema dumped.",
            "OK: total 20 table(s) of stable: meters2 schema dumped.",
            "OK: total 30 table(s) of stable: meters3 schema dumped.",
            "OK: 6024 row(s) dumped out!"
        ]
        self.checkManyString(rlist, results)

    # dump in
    def dumpIn(self, db, newdb, tmpdir):
        cmd = f'-W "{db}={newdb}" -i {tmpdir}'
        rlist = self.taosdump(cmd)
        results = [
            f"rename DB Name {db} to {newdb}",
            f"OK: 6024 row(s) dumped in!"
        ]
        self.checkManyString(rlist, results)

    # super table
    def checkCorrectStb(self, db, newdb):
        #
        # check column
        #
        sqls = [
            # meters1
            [
                f"select (ts) from    {db}.meters1", 
                f"select (ts) from {newdb}.meters1"
            ],
            [
                f"select sum(fc) from    {db}.meters1", 
                f"select sum(fc) from {newdb}.meters1"
            ],
            [
                f"select avg(ic) from    {db}.meters1", 
                f"select avg(ic) from {newdb}.meters1"
            ],
            [
                f"select bin from    {db}.meters1", 
                f"select bin from {newdb}.meters1"
            ],
            # meters2
            [
                f"select (ts) from    {db}.meters2", 
                f"select (ts) from {newdb}.meters2"
            ],
            [
                f"select sum(bi) from    {db}.meters2", 
                f"select sum(bi) from {newdb}.meters2"
            ],
            [
                f"select avg(ui) from    {db}.meters2", 
                f"select avg(ui) from {newdb}.meters2"
            ],
            [
                f"select nch from    {db}.meters2", 
                f"select nch from {newdb}.meters2"
            ],
            # meters3
            [
                f"select (ts) from    {db}.meters3", 
                f"select (ts) from {newdb}.meters3"
            ],
            [
                f"select sum(ti) from    {db}.meters3", 
                f"select sum(ti) from {newdb}.meters3"
            ],
            [
                f"select avg(ui) from    {db}.meters3", 
                f"select avg(ui) from {newdb}.meters3"
            ],
            [
                f"select nch from    {db}.meters3", 
                f"select nch from {newdb}.meters3"
            ]
        ]

        for sql in sqls:
            self.checkSameResult(sql[0], sql[1])

        # new cols is null
        sql = f"select count(*) from {newdb}.meters3 where newic is null"
        tdSql.checkAgg(sql, 3000)

        #
        # check tag
        #
        sqls = [
            [
                f"select distinct tfc,tbname from    {db}.meters3 order by tbname;", 
                f"select distinct tfc,tbname from {newdb}.meters3 order by tbname;"
            ],
            [
                f"select distinct tbin,tbname from    {db}.meters3 order by tbname;", 
                f"select distinct tbin,tbname from {newdb}.meters3 order by tbname;"
            ],
        ]

        for sql in sqls:
            self.checkSameResult(sql[0], sql[1])

        # new tag is null
        sql = f"select count(*) from {newdb}.meters3 where newtdc is null"
        tdSql.checkAgg(sql, 3000)


    # normal table
    def checkCorrectNtb(self, db, newdb):
        pass

    # check correct
    def checkCorrect(self, db, newdb):
        # stb
        self.checkCorrectStb(db, newdb)
        # ntb
        self.checkCorrectNtb(db, newdb)

    def run(self):
        # init
        db    = "dd"
        newdb = "newdd"

        # tmp dir
        tmpdir = "./tmp"
        self.createDir(tmpdir)

        # insert data
        self.insertData()

        # dump out 
        self.dumpOut(db, tmpdir)

        # dump in
        self.dumpIn(db, newdb, tmpdir)

        # check result correct
        self.checkCorrect(db, newdb)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
