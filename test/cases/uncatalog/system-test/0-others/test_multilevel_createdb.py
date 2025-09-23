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


from new_test_framework.utils import tdLog, tdSql, TDSetSql, tdDnodes
import glob
import time

def scanFiles(pattern):
    res = []
    for f in glob.iglob(pattern):
        res += [f]
    return res

def checkFiles(pattern, state):
    res = scanFiles(pattern)
    tdLog.info(res)
    num = len(res)
    if num:
        if state:
            tdLog.info("%s: %d files exist. expect: files exist" % (pattern, num))
        else:
            tdLog.exit("%s: %d files exist. expect: files not exist." % (pattern, num))
    else:
        if state:
            tdLog.exit("%s: %d files exist. expect: files exist" % (pattern, num))
        else:
            tdLog.info("%s: %d files exist. expect: files not exist." % (pattern, num))

class TestMultilevelCreatedb:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()

    def basic(self):
        tdLog.info("============== basic test ===============")
        cfg={
            '/mnt/data1 0 1 0' : 'dataDir',
            '/mnt/data2 0 0 0' : 'dataDir',
            '/mnt/data3 0 0 0' : 'dataDir',
            '/mnt/data4 0 0 0' : 'dataDir'
        }
        tdSql.createDir('/mnt/data1')
        tdSql.createDir('/mnt/data2')
        tdSql.createDir('/mnt/data3')
        tdSql.createDir('/mnt/data4')

        tdDnodes.stop(1)
        time.sleep(3)
        tdDnodes.deploy(1,cfg)
        tdDnodes.start(1)
        
        checkFiles(r'/mnt/data1/*/*',1)
        checkFiles(r'/mnt/data2/*/*',0)
        
        tdSql.execute('create database nws vgroups 20 stt_trigger 1 wal_level 1 wal_retention_period 0')

        checkFiles(r'/mnt/data1/vnode/*/wal',5)
        checkFiles(r'/mnt/data2/vnode/*/wal',5)
        checkFiles(r'/mnt/data3/vnode/*/wal',5)
        checkFiles(r'/mnt/data4/vnode/*/wal',5)

    def test_multilevel_createdb(self):
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
        """
        self.basic()
        tdLog.success("%s successfully executed" % __file__)

