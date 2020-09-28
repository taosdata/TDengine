# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info('=============== step1')
        tdLog.info('create table tb (ts timestamp, speed binary(4089))')
        tdSql.error('create table tb (ts timestamp, speed binary(4089))')
        tdLog.info('create table tb (ts timestamp, speed binary(4088))')
        tdSql.error('create table tb (ts timestamp, speed binary(4088))')
        tdLog.info('create table tb (ts timestamp, speed binary(4084))')
        tdSql.execute('create table tb (ts timestamp, speed binary(4084))')
        tdLog.info("insert into tb values (now, ) -x step1")
        tdSql.error("insert into tb values (now, )")

        with open("../../README.md", "r") as inputFile:
            data = inputFile.read(4084).replace(
                "\n",
                " ").replace(
                "\\",
                " ").replace(
                "\'",
                " ").replace(
                "\"",
                " ").replace(
                    "[",
                    " ").replace(
                        "]",
                        " ").replace(
                            "!",
                " ")

        tdLog.info("insert %d length data: %s" % (len(data), data))

        tdLog.info("insert into tb values (now+2a, data)")
        tdSql.execute("insert into tb values (now+2a, '%s')" % data)
        tdLog.info('select speed from tb order by ts desc')
        tdSql.query('select speed from tb order by ts desc')
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        tdLog.info('==> $data01')
        tdLog.info("tdSql.checkData(0, 1, '%s')" % data)
        tdSql.checkData(0, 1, data)

        tdLog.info(
            'create table tb2 (ts timestamp, speed binary(2040), temp binary(2044))')
        tdSql.execute(
            'create table tb2 (ts timestamp, speed binary(2040), temp binary(2044))')
        speed = inputFile.read(2044).replace(
            "\n",
            " ").replace(
            "\\",
            " ").replace(
            "\'",
            " ").replace(
                "\"",
                " ").replace(
                    "[",
                    " ").replace(
                        "]",
                        " ").replace(
                            "!",
            " ")
        temp = inputFile.read(2040).replace(
            "\n",
            " ").replace(
            "\\",
            " ").replace(
            "\'",
            " ").replace(
                "\"",
                " ").replace(
                    "[",
                    " ").replace(
                        "]",
                        " ").replace(
                            "!",
            " ")
        tdLog.info("insert into tb values (now+3a, speed, temp)")
        tdSql.error(
            "insert into tb values (now+3a, '%s', '%s')" %
            (speed, temp))

        speed = inputFile.read(2040).replace(
            "\n",
            " ").replace(
            "\\",
            " ").replace(
            "\'",
            " ").replace(
                "\"",
                " ").replace(
                    "[",
                    " ").replace(
                        "]",
                        " ").replace(
                            "!",
            " ")
        temp = inputFile.read(2044).replace(
            "\n",
            " ").replace(
            "\\",
            " ").replace(
            "\'",
            " ").replace(
                "\"",
                " ").replace(
                    "[",
                    " ").replace(
                        "]",
                        " ").replace(
                            "!",
            " ")
        tdLog.info("insert into tb values (now+4a, speed, temp)")
        tdSql.error(
            "insert into tb values (now+4a, '%s', '%s')" %
            (speed, temp))

        tdLog.info('tdSql.checkRow(2)')
        tdSql.checkRows(2)
        tdLog.info('==> $data11')
        tdLog.info("tdSql.checkData(1, 1, '%s')" % speed)
        tdSql.checkData(1, 1, speed)

        tdLog.info('==> $data12')
        tdLog.info("tdSql.checkData(1, 2, '%s')" % temp)
        tdSql.checkData(1, 1, temp)

        inputFile.close()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
