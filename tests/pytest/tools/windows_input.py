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
from uiautomation import WindowControl
from util.cases import *
from util.sql import *
import clipboard


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.host = conn._host

    def win_input_test(self):
        os.system("start")
        time.sleep(1)

        # 获取CMD窗口
        # window = DocumentControl(searchDepth=3, Name='Text Area')
        window = WindowControl(searchDepth=1, AutomationId='Console Window')
        time.sleep(1)
        # 切换英文输入法
        # window.SendKeys('\\')
        # window.SendKeys('{Enter}')
        # window.SendKeys('{Shift}')
        # window.SendKeys('\\')
        # window.SendKeys('{Enter}')

        # 切换目录
        window.SendKeys('c:')
        window.SendKeys('{Enter}')
        window.SendKeys('cd \\')
        window.SendKeys('{Enter}')
        window.SendKeys('cd c:\\TDengine')
        window.SendKeys('{Enter}')
        # 启动taos.exe
        window.SendKeys('taos.exe -h %s || taos.exe' % (self.host))
        window.SendKeys('{Enter}')
        # 输入

        temp = ''
        for i in range(300):
            temp += 'a'
        sql = "insert into db.tb values(now,'%s');" % temp
        window.SendKeys(sql)
        window.SendKeys('{Enter}')
        sql = "select * from db.tb;"
        window.SendKeys('{Enter}')
        time.sleep(1)
        window.SendKeys('{Ctrl}A')
        window.SendKeys('{Ctrl}C')
        # 获取剪切板里面的复制内容
        result = clipboard.paste()
        window.SendKeys('{Ctrl}C')
        window.SendKeys('exit')
        window.SendKeys('{Enter}')
        time.sleep(1)
        return result

    def run(self):
        tdSql.prepare()

        ret = tdSql.execute('create table db.tb (ts timestamp, i binary(300))')
        tdSql.execute("insert into db.tb values(now,'sdfsdf')")
        result = self.win_input_test()
        tdLog.info(result)
        time.sleep(5)

        l = tdSql.query("select * from db.tb")
        print(l)
        tdSql.checkRows(1)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
