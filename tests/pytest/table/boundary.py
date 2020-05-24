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

    def getLimitFromSourceCode(self, name):
        cmd = "grep -w '#define %s' ../../src/inc/taosdef.h|awk '{print $3}'" % name
        return int(subprocess.check_output(cmd, shell=True))

    def generateString(self, length):
        chars = string.ascii_uppercase + string.ascii_lowercase
        v = ""
        for i in range(length):
            v += random.choice(chars)
        return v

    def checkTagBoundaries(self):
        tdLog.debug("checking tag boundaries")
        tdSql.prepare()

        maxTags = self.getLimitFromSourceCode('TSDB_MAX_TAGS')
        totalTagsLen = self.getLimitFromSourceCode('TSDB_MAX_TAGS_LEN')
        tdLog.notice("max tags is %d" % maxTags)
        tdLog.notice("max total tag length is %d" % totalTagsLen)

        # for binary tags, 2 bytes are used for length
        tagLen = (totalTagsLen - maxTags * 2) // maxTags
        firstTagLen = totalTagsLen - 2 * maxTags - tagLen * (maxTags - 1)

        sql = "create table cars(ts timestamp, f int) tags(t0 binary(%d)" % firstTagLen
        for i in range(1, maxTags):
            sql += ", t%d binary(%d)" % (i, tagLen)
        sql += ");"

        tdLog.debug("creating super table: " + sql)
        tdSql.execute(sql)
        tdSql.query('show stables')
        tdSql.checkRows(1)

        for i in range(10):
            sql = "create table car%d using cars tags('%d'" % (i, i)
            sql += ", '0'" * (maxTags - 1) + ");"
            tdLog.debug("creating table: " + sql)
            tdSql.execute(sql)

            sql = "insert into car%d values(now, 0);" % i
            tdLog.debug("inserting data: " + sql)
            tdSql.execute(sql)

        tdSql.query('show tables')
        tdLog.info('tdSql.checkRow(10)')
        tdSql.checkRows(10)

        tdSql.query('select * from cars;')
        tdSql.checkRows(10)

    def checkColumnBoundaries(self):
        tdLog.debug("checking column boundaries")
        tdSql.prepare()

        # one column is for timestamp
        maxCols = self.getLimitFromSourceCode('TSDB_MAX_COLUMNS') - 1

        sql = "create table cars (ts timestamp"
        for i in range(maxCols):
            sql += ", c%d int" % i
        sql += ");"
        tdSql.execute(sql)
        tdSql.query('show tables')
        tdSql.checkRows(1)

        sql = "insert into cars values (now"
        for i in range(maxCols):
            sql += ", %d" % i
        sql += ");"
        tdSql.execute(sql)
        tdSql.query('select * from cars')
        tdSql.checkRows(1)

    def checkTableNameBoundaries(self):
        tdLog.debug("checking table name boundaries")
        tdSql.prepare()

        maxTableNameLen = self.getLimitFromSourceCode('TSDB_TABLE_NAME_LEN')
        tdLog.notice("table name max length is %d" % maxTableNameLen)

        # create a super table with name exceed max length
        sname = self.generateString(maxTableNameLen + 1)
        tdLog.info("create a super table with length %d" % len(sname))
        tdSql.error("create table %s (ts timestamp, value int) tags(id int)" % sname)

        # create a super table with name of max length
        sname = self.generateString(maxTableNameLen)
        tdLog.info("create a super table with length %d" % len(sname))
        tdSql.execute("create table %s (ts timestamp, value int) tags(id int)" % sname)
        tdLog.info("check table count, should be one")
        tdSql.query('show stables')
        tdSql.checkRows(1)

        # create a child table with name exceed max length
        name = self.generateString(maxTableNameLen + 1)
        tdLog.info("create a child table with length %d" % len(name))
        tdSql.error("create table %s using %s tags(0)" % (name, sname))

        # create a child table with name of max length
        name = self.generateString(maxTableNameLen)
        tdLog.info("create a child table with length %d" % len(name))
        tdSql.execute("create table %s using %s tags(0)" % (name, sname))
        tdSql.query('show tables')
        tdSql.checkRows(1)

        # insert one row
        tdLog.info("insert one row of data")
        tdSql.execute("insert into %s values(now, 0)" % name)
        tdSql.query("select * from " + name)
        tdSql.checkRows(1)
        tdSql.query("select * from " + sname)
        tdSql.checkRows(1)

        name = name[:len(name) - 1]
        tdSql.error("select * from " + name)
        tdSql.checkRows(0)

    def checkRowBoundaries(self):
        tdLog.debug("checking row boundaries")
        tdSql.prepare()

        # 8 bytes for timestamp
        maxRowSize = 65536 - 8
        maxCols = self.getLimitFromSourceCode('TSDB_MAX_COLUMNS') - 1

        # for binary cols, 2 bytes are used for length
        colLen = (maxRowSize - maxCols * 2) // maxCols
        firstColLen = maxRowSize - 2 * maxCols - colLen * (maxCols - 1)

        sql = "create table cars (ts timestamp, c0 binary(%d)" % firstColLen
        for i in range(1, maxCols):
            sql += ", c%d binary(%d)" % (i, colLen)
        sql += ");"
        tdSql.execute(sql)
        tdSql.query('show tables')
        tdSql.checkRows(1)

        col = self.generateString(firstColLen)
        sql = "insert into cars values (now, '%s'" % col
        col = self.generateString(colLen)
        for i in range(1, maxCols):
            sql += ", '%s'" % col
        sql += ");"
        tdLog.info(sql)
        tdSql.execute(sql)
        tdSql.query("select * from cars")
        tdSql.checkRows(1)

    def run(self):
        self.checkTagBoundaries()
        self.checkColumnBoundaries()
        self.checkTableNameBoundaries()
        self.checkRowBoundaries()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
