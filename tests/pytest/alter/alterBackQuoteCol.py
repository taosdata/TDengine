
# -*- coding: utf-8 -*-

import random
import string
import subprocess
import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
    def run(self):
        tdLog.debug("check databases")
        tdSql.prepare()

        ### test normal table
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")
        tdSql.execute("create stable `sch.job.create` (`ts` TIMESTAMP, `tint` int, `node.value` NCHAR(7)) TAGS (`endpoint` NCHAR(7),`task.type` NCHAR(3))")
        tdSql.execute("alter table `sch.job.create` modify tag `task.type` NCHAR(4)")
        tdSql.execute("alter table `sch.job.create` change tag `task.type` `chan.type`")
        tdSql.execute("alter table `sch.job.create` drop tag `chan.type`")
        tdSql.execute("alter table `sch.job.create` add tag `add.type` NCHAR(6)")
        tdSql.query("describe `sch.job.create`")
        tdSql.checkData(4, 0, "add.type")

        tdSql.execute("alter table `sch.job.create` modify column `node.value` NCHAR(8)")
        tdSql.execute("alter table `sch.job.create` drop column `node.value`")
        tdSql.execute("alter table `sch.job.create` add column `add.value` NCHAR(6)")

        tdSql.query("describe `sch.job.create`")
        tdSql.checkData(2, 0, "add.value")

        tdSql.execute("insert into `tsch.job.create` using `sch.job.create`(`add.type`) TAGS('tag1') values(now, 1, 'here')")
        tdSql.execute("alter table `tsch.job.create` set tag `add.type` = 'tag2'")
        tdSql.query("select `add.type` from `tsch.job.create`")
        tdSql.checkData(0, 0, "tag2")

        ### test stable
        tdSql.execute("create stable `ssch.job.create` (`ts` TIMESTAMP, `tint` int, `node.value` NCHAR(7)) TAGS (`endpoint` NCHAR(7),`task.type` NCHAR(3))")
        tdSql.execute("alter stable `ssch.job.create` modify tag `task.type` NCHAR(4)")
        tdSql.execute("alter stable `ssch.job.create` change tag `task.type` `chan.type`")
        tdSql.execute("alter stable `ssch.job.create` drop tag `chan.type`")
        tdSql.execute("alter stable `ssch.job.create` add tag `add.type` NCHAR(6)")
        tdSql.query("describe `ssch.job.create`")
        tdSql.checkData(4, 0, "add.type")

        tdSql.execute("alter stable `ssch.job.create` modify column `node.value` NCHAR(8)")
        tdSql.execute("alter stable `ssch.job.create` drop column `node.value`")
        tdSql.execute("alter stable `ssch.job.create` add column `add.value` NCHAR(6)")

        tdSql.query("describe `ssch.job.create`")
        tdSql.checkData(2, 0, "add.value")

        tdSql.execute("insert into `tssch.job.create` using `ssch.job.create`(`add.type`) TAGS('tag1') values(now, 1, 'here')")
        tdSql.error("alter stable `tssch.job.create` set tag `add.type` = 'tag2'")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())