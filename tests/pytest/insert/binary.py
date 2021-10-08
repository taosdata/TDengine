# -*- coding: utf-8 -*-

import platform
import sys
from util.log import *
from util.cases import *
from util.sql import *
import subprocess
import os


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info('=============== step1')
        tdLog.info('create table tb (ts timestamp, speed binary(5))')
        tdSql.execute('create table tb (ts timestamp, speed binary(5))')
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
        tdLog.info("insert into tb values (now+2a, '23456')")
        tdSql.execute("insert into tb values (now+2a, '23456')")
        tdLog.info('select speed from tb order by ts desc')
        tdSql.query('select speed from tb order by ts desc')
        tdLog.info('tdSql.checkRow(2)')
        tdSql.checkRows(2)
        tdLog.info('==> $data00')
        tdLog.info("tdSql.checkData(0, 0, '23456')")
        tdSql.checkData(0, 0, '23456')
        tdLog.info('=============== step4')
        tdLog.info("insert into tb values (now+3a, '345678')")
        tdSql.error("insert into tb values (now+3a, '345678')")
        tdLog.info("insert into tb values (now+3a, '34567')")
        tdSql.execute("insert into tb values (now+3a, '34567')")
        tdLog.info('select speed from tb order by ts desc')
        tdSql.query('select speed from tb order by ts desc')
        tdLog.info('tdSql.checkRow(3)')
        tdSql.checkRows(3)
        tdLog.info('==> $data00')
        tdLog.info("tdSql.checkData(0, 0, '34567')")
        tdSql.checkData(0, 0, '34567')
        tdLog.info("insert into tb values (now+4a, \"'';\")")
        if platform.system() == "Linux":
            config_dir = subprocess.check_output(str("ps -ef |grep dnode1|grep -v grep |awk '{print $NF}'"), stderr=subprocess.STDOUT, shell=True).decode('utf-8').replace('\n', '')
            result = ''.join(os.popen(r"""taos  -s "insert into db.tb values (now+4a, \"'';\")" -c %s"""%(config_dir)).readlines())
            if "Query OK" not in result: tdLog.exit("err:insert '';")            
        tdLog.info('drop database db')
        tdSql.execute('drop database db')
        tdLog.info('show databases')
        tdSql.query('show databases')
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
# convert end

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
