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

        tdSql.execute('create table xcxlog (ts timestamp, user_id int, path BINARY(40),scene int) tags(appid bigint, adzone_id int,ip bigint,session_id bigint)')
        tdSql.error("insert into d1000004(user_id,path,scene,ts) using xcxlog tags(1000004,145,97160) values (97160,'pagex/goods/taoke',1086,now)")
        tdSql.execute("insert into d1000004_145(user_id,path,scene,ts) using xcxlog(appid,adzone_id,session_id,ip) tags(1000004,145,97160,1717171445) values (97160,'pagex/goods/taoke',1086,now)")
        
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'd1000004_145')

        tdSql.query("select * from xcxlog")
        tdSql.checkRows(1)
        tdSql.checkData(0, 4, 1000004)
        tdSql.checkData(0, 5, 145)
        tdSql.checkData(0, 6, 1717171445)
        tdSql.checkData(0, 7, 97160)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
