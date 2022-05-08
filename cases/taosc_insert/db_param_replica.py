###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from taostest import TDCase, T
from taostest.util.common import TDCom

class TestReplica(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def replica_check(self):
        """
        replica check
        """
        test_param = "replica"
        # default
        default_value = 1
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        param_value_list = [1, 3]
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            if param_value == 1:
                self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
                self.tdSql.query('show databases')
                db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
                self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
                self.tdSql.execute(f'drop database {dbname}')
            else:
                pass

    def alter_db(self):
        """
        alter replica
        """
        self.tdSql.drop_all_db()
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        # blocks
        self.tdSql.execute(f'alter database {dbname} blocks 12')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(int(res[0][9]), 12)
        # wal
        self.tdSql.execute(f'alter database {dbname} wal 2')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(int(res[0][12]), 2)
        # fsync
        self.tdSql.execute(f'alter database {dbname} fsync 1000')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        self.tdSql.checkEqual(int(res[0][13]), 1000)
        # keep
        self.tdSql.execute(f'alter database {dbname} keep 36500')
        self.tdSql.query('show databases')
        res = self.tdSql.getOneRow(0, dbname)
        if str(res[0][7]) == '36500':
            self.tdSql.checkEqual(int(res[0][7]), 36500)
        elif str(res[0][7]) == '36500,36500,36500':
            self.tdSql.checkEqual(str(res[0][7]), '36500,36500,36500')
        else:
            self.tdSql.checkEqual(str(res[0][7]), 'unexpected value')
        # # replica
        # out of dnodes
        # for replica in [2, 1]:
        #     self.tdSql.execute(f'alter database {dbname} replica {replica}')
        #     self.tdSql.execute('show databases')
        #     res = self.tdSql.getOneRow(0, dbname)
        #     self.tdSql.checkEqual(res[0][4], replica)
        # quorum
        # Database options not changed
        # for quorum in [1, 2]:
        #     self.tdSql.execute(f'alter database {dbname} quorum {quorum}')
        #     self.tdSql.execute('show databases')
        #     res = self.tdSql.getOneRow(0, dbname)
        #     self.tdSql.checkEqual(res[0][5], quorum)
        # cachelast
        for cachelast in [1, 0]:
            self.tdSql.execute(f'alter database {dbname} cachelast {cachelast}')
            self.tdSql.query('show databases')
            res = self.tdSql.getOneRow(0, dbname)
            self.tdSql.checkEqual(res[0][15], cachelast)


    def run(self) -> bool:
        self.replica_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            replica check <jayden>: [TD-14991] : replica check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

