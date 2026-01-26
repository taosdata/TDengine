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
from new_test_framework.utils import tdLog, tdSql, TDSetSql
import time
import taos
from taos.tmq import *


class TestSubscribeStreamPrivilege:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 143, 'clientCfg':clientCfgDict}

    def setup_class(cls):
        tdLog.info("start to execute %s" % __file__)
        cls.setsql = TDSetSql()
        cls.stbname = 'stb'
        cls.user_name = 'test'
        cls.binary_length = 20  # the length of binary for column_dict
        cls.nchar_length = 20  # the length of nchar for column_dict
        cls.dbnames = ['db1']
        cls.column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
            'col3': 'float',
        }
        
        cls.tag_dict = {
            't1': 'int',
            't2': f'binary({cls.binary_length})'
        }
        
        cls.tag_list = [
            f'1, "Beijing"',
            f'2, "Shanghai"',
            f'3, "Guangzhou"',
            f'4, "Shenzhen"'
        ]
        
        cls.values_list = [
            f'now, 9.1, 200, 0.3'            
        ]
        
        cls.tbnum = 4
        cls.topic_name = 'topic1'


    def prepare_data(self):
        for db in self.dbnames:
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"use {db}")
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
                for j in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values({j})')

    def checkUserPrivileges(self, rowCnt):
        tdSql.query("show user privileges")
        print(tdSql.queryResult)
        tdSql.checkRows(rowCnt)

    def consumeTest(self):
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": self.user_name,
            "td.connect.pass": "123456rf@#",
            "auto.offset.reset": "earliest"
        }
        consumer = Consumer(consumer_dict)

        tdLog.info("test subscribe topic created by other user")
        exceptOccured = False
        try:
            consumer.subscribe([self.topic_name])
        except TmqError:
            exceptOccured = True

        if not exceptOccured:
            tdLog.exit(f"has no privilege, should except")

        self.checkUserPrivileges(0)
        tdLog.info("test subscribe topic privilege granted by other user")
        tdSql.execute(f'grant subscribe on topic {self.dbnames[0]}.{self.topic_name} to {self.user_name}')
        tdSql.execute(f'grant use on database {self.dbnames[0]} to {self.user_name}')
        self.checkUserPrivileges(2)

        exceptOccured = False
        try:
            consumer.subscribe([self.topic_name])
        except TmqError:
            exceptOccured = True

        if exceptOccured:
            tdLog.exit(f"has privilege, should not except")

        cnt = 0
        try:
            while True:
                res = consumer.poll(1)
                cnt += 1
                if cnt == 1:
                    if not res:
                        tdLog.exit(f"grant privilege, should get res")
                elif cnt == 2:
                    if res:
                        tdLog.exit(f"revoke privilege, should get NULL")
                    else:
                        break

                tdLog.info("test subscribe topic privilege revoked by other user")
                tdSql.execute(f'revoke subscribe on topic {self.dbnames[0]}.{self.topic_name} from {self.user_name}')
                tdSql.execute(f'revoke use on database {self.dbnames[0]} from {self.user_name}')
                self.checkUserPrivileges(0)
                time.sleep(5)

        finally:
            consumer.close()

    def create_user(self):
        tdSql.execute(f'create topic {self.topic_name} as database {self.dbnames[0]}')
        tdSql.execute(f'create user {self.user_name} pass "123456rf@#"')

    def test_priv_subscribe(self):
        """Privileges subscribe
        
        1. Prepare 1 database, 1 super table, 4 child tables
        2. Insert data into child tables
        3. Create topic on the database by admin user
        4. Create normal user
        5. Test subscribe topic privilege without granted
        6. Grant subscribe privilege on the topic to normal user
        7. Test subscribe topic privilege after granted
        8. Revoke subscribe privilege on the topic from normal user
        9. Test subscribe topic privilege after revoked
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-02 Alex Duan Migrated from uncatalog/system-test/0-others/test_subscribe_stream_privilege.py

        """
        self.prepare_data()
        self.create_user()
        self.consumeTest()

        tdLog.success("%s " % __file__)


