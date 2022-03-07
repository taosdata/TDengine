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

import random
import string
from util.sql import tdSql
from util.dnodes import tdDnodes

class TDCom:
    def init(self, conn, logSql):
        tdSql.init(conn.cursor(), logSql)

    def cleanTb(self):
        query_sql = "show stables"
        res_row_list = tdSql.query(query_sql, True)
        stb_list = map(lambda x: x[0], res_row_list)
        for stb in stb_list:
            tdSql.execute(f'drop table if exists {stb}')

        query_sql = "show tables"
        res_row_list = tdSql.query(query_sql, True)
        tb_list = map(lambda x: x[0], res_row_list)
        for tb in tb_list:
            tdSql.execute(f'drop table if exists {tb}')

    def getLongName(self, len, mode = "mixed"):
        """
            generate long name
            mode could be numbers/letters/letters_mixed/mixed
        """    
        if mode == "numbers": 
            chars = ''.join(random.choice(string.digits) for i in range(len))
        elif mode == "letters": 
            chars = ''.join(random.choice(string.ascii_letters.lower()) for i in range(len))
        elif mode == "letters_mixed": 
            chars = ''.join(random.choice(string.ascii_letters.upper() + string.ascii_letters.lower()) for i in range(len))
        else:
            chars = ''.join(random.choice(string.ascii_letters.lower() + string.digits) for i in range(len))
        return chars

    def restartTaosd(self, index=1, db_name="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use {db_name}")

    def close(self):
        self.cursor.close()

tdCom = TDCom()
