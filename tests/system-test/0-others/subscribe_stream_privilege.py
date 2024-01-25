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
import time

import taos
from taos.tmq import *
from util.cases import *
from util.common import *
from util.log import *
from util.sql import *
from util.sqlset import *


class TDTestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'clientCfg':clientCfgDict}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.stbname = 'stb'
        self.user_name = 'test'
        self.binary_length = 20  # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.dbnames = ['db1']
        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
            'col3': 'float',
        }
        
        self.tag_dict = {
            't1': 'int',
            't2': f'binary({self.binary_length})'
        }
        
        self.tag_list = [
            f'1, "Beijing"',
            f'2, "Shanghai"',
            f'3, "Guangzhou"',
            f'4, "Shenzhen"'
        ]
        
        self.values_list = [
            f'now, 9.1, 200, 0.3'            
        ]
        
        self.tbnum = 4
        self.topic_name = 'topic1'


    def prepare_data(self):
        for db in self.dbnames:
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"use {db}")
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
                for j in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values({j})')

    def consumeTest(self):
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": self.user_name,
            "td.connect.pass": "test",
            "auto.offset.reset": "earliest"
        }
        consumer = Consumer(consumer_dict)

        tdLog.debug("test subscribe topic created by other user")
        exceptOccured = False
        try:
            consumer.subscribe([self.topic_name])
        except TmqError:
            exceptOccured = True

        if not exceptOccured:
            tdLog.exit(f"has no privilege, should except")

        tdLog.debug("test subscribe topic privilege granted by other user")
        tdSql.execute(f'grant subscribe on {self.topic_name} to {self.user_name}')

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

                tdLog.debug("test subscribe topic privilege revoked by other user")
                tdSql.execute(f'revoke subscribe on {self.topic_name} from {self.user_name}')
                time.sleep(5)

        finally:
            consumer.close()

    def create_user(self):
        tdSql.execute(f'create topic {self.topic_name} as database {self.dbnames[0]}')
        tdSql.execute(f'create user {self.user_name} pass "test"')

    def run(self):
        self.prepare_data()
        self.create_user()
        self.consumeTest()
                
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())