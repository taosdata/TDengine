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

import taos
from taos import *
from stmt.common import StmtCommon
from taos.constants import FieldType
from ctypes import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase:
    def init(self, conn, log_sql, replica_var=1):
        self.replica_var = int(replica_var)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = "stmt_query_test_cases"
        self.stmt_common = StmtCommon()
        self.connectstmt = None

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

    def newcon(self, host, cfg):
        user = "root"
        password = "taosdata"
        port = 6030
        con = taos.connect(host=host, user=user, password=password, config=cfg, port=port)
        tdLog.debug(con)
        return con

    def test_func_statement2_param_sql(self):
        try:
            self.connectstmt.statement2(None)
            self.connectstmt.statement2('')
            self.connectstmt.statement2('123')
            self.connectstmt.statement2('-123')
            self.connectstmt.statement2('3.14')
            self.connectstmt.statement2('-3.14')
            self.connectstmt.statement2('abc')
            self.connectstmt.statement2('hello')
            self.connectstmt.statement2(';')
            self.connectstmt.statement2(',;')
            self.connectstmt.statement2('select * from common_table;')
            self.connectstmt.statement2('selct * from common_table where ts > 1;')
            self.connectstmt.statement2('selct * from common_table where ts > ?;')
            self.connectstmt.statement2('selct * from ? where ts > ?;')
            self.connectstmt.statement2('selct * frm ? where ts > ?;')
            self.connectstmt.statement2('select count(*) from common_table;')
        except Exception as err:
            tdLog.exit(f"An unexpected error occurred, err: {err}")

    def test_func_statement2_param_option(self):
        option = TaosStmt2Option()
        self.connectstmt.statement2(option=option)

    def test_internal_func_get_impl(self):
        option = TaosStmt2Option(0)
        impl = option.get_impl()
        assert impl is not None

        option = TaosStmt2Option(0, False, False)
        impl = option.get_impl()
        assert impl is not None
        self.connectstmt.statement2('select * from common_table;', option=option)

        option = TaosStmt2Option(0, False, True)
        impl = option.get_impl()
        assert impl is not None
        self.connectstmt.statement2('select * from common_table;', option=option)

        option = TaosStmt2Option(0, True, False)
        impl = option.get_impl()
        assert impl is not None
        self.connectstmt.statement2('select * from common_table;', option=option)

        option = TaosStmt2Option(0, True, True)
        impl = option.get_impl()
        assert impl is not None
        self.connectstmt.statement2('select * from common_table;', option=option)

    def run(self):
        build_path = self.stmt_common.getBuildPath()
        config = build_path + "../sim/dnode1/cfg/"
        host = "localhost"
        self.connectstmt = self.newcon(host, config)

        self.test_func_statement2_param_sql()
        # self.test_func_statement2_param_option()
        self.test_internal_func_get_impl()

        self.connectstmt.close()
        return


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
