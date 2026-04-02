# -*- coding: utf-8 -*-

import platform
import sys
from util.log import *
from util.cases import *
from util.sql import *
import subprocess
import os


class TDTestCase:
    def init(self, conn, logSql, replicaVar = 1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taos"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        return paths[0]

    def run(self):
        tdSql.prepare()

        tdLog.info('=============== step1')
        tdLog.info('create table tb (ts timestamp, speed binary(10))')
        tdSql.execute('create table tb (ts timestamp, speed binary(10))')
        tdLog.info("insert into tb values (now, ) -x step1")
        tdSql.error("insert into tb values (now, )")
        tdLog.info('=============== step2')
        tdLog.info("insert into tb values (now+1a, '1234')")
        tdSql.execute("insert into tb values (now+1a, '1234')")
        tdLog.info('select speed from tb order by ts desc')
        tdSql.query('select speed from tb order by ts desc')
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        tdLog.info("tdSql.checkData(0, 0, '1234')")
        tdSql.checkData(0, 0, '1234')
        tdLog.info('=============== step3')
        tdLog.info("insert into tb values (now+2a, '0123456789')")
        tdSql.execute("insert into tb values (now+2a, '0123456789')")
        tdLog.info('select speed from tb order by ts desc')
        tdSql.query('select speed from tb order by ts desc')
        tdLog.info('tdSql.checkRow(2)')
        tdSql.checkRows(2)
        tdLog.info('==> $data00')
        tdLog.info("tdSql.checkData(0, 0, '0123456789')")
        tdSql.checkData(0, 0, '0123456789')
        tdLog.info('=============== step4')
        tdLog.info("insert into tb values (now+3a, '01234567890')")
        tdSql.error("insert into tb values (now+3a, '01234567890')")
        tdLog.info("insert into tb values (now+3a, '34567')")
        tdSql.execute("insert into tb values (now+3a, '34567')")
        tdLog.info("insert into tb values (now+4a, NULL)")
        tdSql.execute("insert into tb values (now+4a, NULL)")
        tdLog.info('select speed from tb order by ts desc')
        tdSql.query('select speed from tb order by ts desc')
        tdSql.checkRows(4)
        tdLog.info("tdSql.checkData(0, 0, '0123456789')")
        tdSql.checkData(0, 0, '0123456789')
        tdLog.info("tdSql.checkData(3, 0, None)")
        tdSql.checkData(3, 0, None)
        tdLog.info("insert into tb values (now+4a, \"'';\")")

        if platform.system() == "Linux":
            config_dir = subprocess.check_output(
                str("ps -ef |grep dnode1|grep -v grep |awk '{print $NF}'"),
                stderr=subprocess.STDOUT,
                shell=True).decode('utf-8').replace(
                    '\n',
                    '')

            binPath = self.getPath("taos")
            if (binPath == ""):
                tdLog.exit("taos not found!")
            else:
                tdLog.info("taos found: %s" % binPath)

            result = ''.join(
                os.popen(
                    r"""%s -s "insert into db.tb values (now+4a, \"'';\")" -c %s""" %
                    (binPath, (config_dir))).readlines())
            if "Query OK" not in result:
                tdLog.exit("err:insert '';")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
