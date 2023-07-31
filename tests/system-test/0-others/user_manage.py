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
from taos.tmq import *
from util.cases import *
from util.common import *
from util.log import *
from util.sql import *
from util.sqlset import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.stbname = 'stb'
        self.binary_length = 20  # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': f'binary({self.binary_length})',
            'col13': f'nchar({self.nchar_length})'
        }
        self.tag_dict = {
            'ts_tag': 'timestamp',
            't1': 'tinyint',
            't2': 'smallint',
            't3': 'int',
            't4': 'bigint',
            't5': 'tinyint unsigned',
            't6': 'smallint unsigned',
            't7': 'int unsigned',
            't8': 'bigint unsigned',
            't9': 'float',
            't10': 'double',
            't11': 'bool',
            't12': f'binary({self.binary_length})',
            't13': f'nchar({self.nchar_length})'
        }
        self.tag_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        self.values_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        self.tbnum = 1

    def prepare_data(self):
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')

    def create_user(self):
        for user_name in ['jiacy1_all', 'jiacy1_read', 'jiacy1_write', 'jiacy1_none', 'jiacy0_all', 'jiacy0_read',
                          'jiacy0_write', 'jiacy0_none']:
            if 'jiacy1' in user_name.lower():
                tdSql.execute(f'create user {user_name} pass "123" sysinfo 1')
            elif 'jiacy0' in user_name.lower():
                tdSql.execute(f'create user {user_name} pass "123" sysinfo 0')
        for user_name in ['jiacy1_all', 'jiacy1_read', 'jiacy0_all', 'jiacy0_read']:
            tdSql.execute(f'grant read on db to {user_name}')
        for user_name in ['jiacy1_all', 'jiacy1_write', 'jiacy0_all', 'jiacy0_write']:
            tdSql.execute(f'grant write on db to {user_name}')

    def user_privilege_check(self):
        jiacy1_read_conn = taos.connect(user='jiacy1_read', password='123')
        sql = "create table ntb (ts timestamp,c0 int)"
        expectErrNotOccured = True
        try:
            jiacy1_read_conn.execute(sql)
        except BaseException:
            expectErrNotOccured = False
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        else:
            self.queryRows = 0
            self.queryCols = 0
            self.queryResult = None
            tdLog.info(f"sql:{sql}, expect error occured")
        pass

    def drop_topic(self):
        jiacy1_all_conn = taos.connect(user='jiacy1_all', password='123')
        jiacy1_read_conn = taos.connect(user='jiacy1_read', password='123')
        jiacy1_write_conn = taos.connect(user='jiacy1_write', password='123')
        jiacy1_none_conn = taos.connect(user='jiacy1_none', password='123')
        jiacy0_all_conn = taos.connect(user='jiacy0_all', password='123')
        jiacy0_read_conn = taos.connect(user='jiacy0_read', password='123')
        jiacy0_write_conn = taos.connect(user='jiacy0_write', password='123')
        jiacy0_none_conn = taos.connect(user='jiacy0_none', password='123')
        tdSql.execute('create topic root_db as select * from db.stb')
        for user in [jiacy1_all_conn, jiacy1_read_conn, jiacy0_all_conn, jiacy0_read_conn]:
            user.execute(f'create topic db_jiacy as select * from db.stb')
            user.execute('drop topic db_jiacy')
        for user in [jiacy1_write_conn, jiacy1_none_conn, jiacy0_write_conn, jiacy0_none_conn, jiacy1_all_conn,
                     jiacy1_read_conn, jiacy0_all_conn, jiacy0_read_conn]:
            sql_list = []
            if user in [jiacy1_all_conn, jiacy1_read_conn, jiacy0_all_conn, jiacy0_read_conn]:
                sql_list = ['drop topic root_db']
            elif user in [jiacy1_write_conn, jiacy1_none_conn, jiacy0_write_conn, jiacy0_none_conn]:
                sql_list = ['drop topic root_db', 'create topic db_jiacy as select * from db.stb']
            for sql in sql_list:
                expectErrNotOccured = True
                try:
                    user.execute(sql)
                except BaseException:
                    expectErrNotOccured = False
                if expectErrNotOccured:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
                else:
                    self.queryRows = 0
                    self.queryCols = 0
                    self.queryResult = None
                    tdLog.info(f"sql:{sql}, expect error occured")

    def tmq_commit_cb_print(tmq, resp, param=None):
        print(f"commit: {resp}, tmq: {tmq}, param: {param}")

    def subscribe_topic(self):
        print("create topic")
        tdSql.execute('create topic db_topic as select * from db.stb')
        tdSql.execute('grant subscribe on db_topic to jiacy1_all')
        print("build consumer")
        tmq = Consumer({"group.id": "tg2", "td.connect.user": "jiacy1_all", "td.connect.pass": "123",
                        "enable.auto.commit": "true"})
        print("build topic list")
        tmq.subscribe(["db_topic"])
        print("basic consume loop")
        c = 0
        l = 0
        for i in range(10):
            if c > 10:
                break
            res = tmq.poll(10)
            print(f"loop {l}")
            l += 1
            if not res:
                print(f"received empty message at loop {l} (committed {c})")
                continue
            if res.error():
                print(f"consumer error at loop {l} (committed {c}) {res.error()}")
                continue

            c += 1
            topic = res.topic()
            db = res.database()
            print(f"topic: {topic}\ndb: {db}")

            for row in res:
                print(row.fetchall())
            print("* committed")
            tmq.commit(res)

    def run(self):
        tdSql.prepare()
        self.create_user()
        self.prepare_data()
        self.drop_topic()
        self.user_privilege_check()
        self.subscribe_topic()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
