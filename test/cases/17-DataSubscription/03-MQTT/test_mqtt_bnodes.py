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
import time
from new_test_framework.utils import tdLog, tdSql, tdMqtt, sc, clusterComCheck


class TestMqttBnodes:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mqtt_bnodes(self):
        """MQTT: bnodes test

        Bnodes testing.
        -N 2~11

        Catalog:
            - Subscribe:Mqtt

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-27 Created by stephenkgu

        """

        self.create_bnodes()
        self.show_bnodes()
        self.drop_bnodes()
        self.create_drop_repeat()

    def create_bnodes(self):
        tdLog.info(f"test bnodes creating")

        tdMqtt.dropAllTopicsDbsAndBnodes()

        tdSql.query("show dnodes")
        dnodes_count = tdSql.getRows();

        if dnodes_count > 1:
            self.bnodes_count = 2
        else:
            self.bnodes_count = 1
        for i in range(self.bnodes_count):
            tdMqtt.createBnode(i + 1)

        tdSql.query("show bnodes")
        tdSql.checkKeyExist(self.bnodes_count)

        dnode_idx = dnodes_count + 1;
        tdSql.error(f"create bnode on dnode {dnode_idx}")
        dnode_idx = 0
        tdSql.error(f"create bnode on dnode {dnode_idx}")
        dnode_idx = -1
        tdSql.error(f"create bnode on dnode {dnode_idx}")
        dnode_idx = -10
        tdSql.error(f"create bnode on dnode {dnode_idx}")
        dnode_idx = dnodes_count + 10
        tdSql.error(f"create bnode on dnode {dnode_idx}")
        
    def show_bnodes(self):
        tdLog.info(f"test show bnodes")

        tdSql.query("show bnodes")
        tdSql.checkKeyExist(self.bnodes_count)
        for i in range(self.bnodes_count):
            tdSql.checkData(i, 0, i + 1)
            tdSql.checkData(i, 2, "mqtt")
    
    def drop_bnodes(self):
        tdLog.info(f"test bnodes dropping")

        tdSql.error(f"drop bnode on dnode 0")
        tdSql.error(f"drop bnode on dnode -1")
        tdSql.error(f"drop bnode on dnode -10")
        tdSql.error(f"drop bnode on dnode {self.bnodes_count + 1}")
        tdSql.error(f"drop bnode on dnode {self.bnodes_count + 10}")

        tdSql.query("show bnodes")
        tdSql.checkKeyExist(self.bnodes_count)
        for i in range(self.bnodes_count):
            tdMqtt.dropBnode(i+1);

        tdSql.error(f"drop bnode on dnode {1}")
        tdSql.error(f"drop bnode on dnode {self.bnodes_count}")
        tdSql.error(f"drop bnode on dnode 0")
        tdSql.error(f"drop bnode on dnode -1")
        tdSql.error(f"drop bnode on dnode -10")
        tdSql.error(f"drop bnode on dnode {self.bnodes_count + 1}")
        tdSql.error(f"drop bnode on dnode {self.bnodes_count + 10}")
            
    def create_drop_repeat(self):
        tdLog.info(f"test bnodes creating")

        tdMqtt.dropAllTopicsDbsAndBnodes()

        tdSql.query("show dnodes")
        dnodes_count = tdSql.getRows();

        target_dnode = 1
        repeat_count = 10
        for i in range(repeat_count):
            tdMqtt.createBnode(target_dnode)
            tdSql.query("show bnodes")
            tdSql.checkKeyExist(target_dnode)
            tdSql.query("select * from information_schema.ins_bnodes")
            tdSql.checkKeyExist(target_dnode)
            tdMqtt.dropBnode(target_dnode)

        target_dnode = dnodes_count
        for i in range(repeat_count):
            tdMqtt.createBnode(target_dnode)
            tdSql.query("show bnodes")
            tdSql.checkKeyExist(target_dnode)
            tdSql.query("select * from information_schema.ins_bnodes")
            tdSql.checkKeyExist(target_dnode)
            tdMqtt.dropBnode(target_dnode)

        clusterComCheck.checkDnodes(dnodes_count)

        # create bnode on first dnode
        
        target_dnode = 1
        tdMqtt.createBnode(target_dnode)
        tdSql.query("show bnodes")
        tdSql.checkKeyExist(target_dnode)
        tdSql.query("select * from information_schema.ins_bnodes")
        tdSql.checkKeyExist(target_dnode)
        sc.dnodeStop(target_dnode);
        sc.dnodeStart(target_dnode);
        tdSql.query("show bnodes")
        tdSql.checkKeyExist(target_dnode)
        tdSql.query("select * from information_schema.ins_bnodes")
        tdSql.checkKeyExist(target_dnode)

        # create bnode on last dnode
        
        tdMqtt.dropBnode(target_dnode)
        target_dnode = min(10, dnodes_count)
        tdMqtt.createBnode(target_dnode)
        tdSql.query("show bnodes")
        tdSql.checkKeyExist(target_dnode)
        tdSql.query("select * from information_schema.ins_bnodes")
        tdSql.checkKeyExist(target_dnode)
        sc.dnodeStop(target_dnode);
        sc.dnodeStart(target_dnode);
        tdSql.query("show bnodes")
        tdSql.checkKeyExist(target_dnode)
        tdSql.query("select * from information_schema.ins_bnodes")
        tdSql.checkKeyExist(target_dnode)

        #sc.dnodeStop(target_dnode);
        #tdSql.error(f"drop bnode on dnode {target_dnode}")
        # tdSql.error(f"drop bnode on dnode {target_dnode}", 0x800003D5)
        # tdSql.error(f"drop bnode on dnode {target_dnode}", 0x80000408)
        #sc.dnodeStart(target_dnode);
        
        tdSql.error(f"drop dnode {target_dnode}");
        # tdSql.error(f"drop dnode {target_dnode}", 0x800003d3)
        # tdSql.error(f"drop dnode {target_dnode}", 0x80000138)
        tdMqtt.dropBnode(target_dnode)
        tdSql.query(f"drop dnode {target_dnode}")

    def basic_case(self):
        # ---- global parameters start ----#
        dbName = "power"
        vgroups = 1
        precision = "us"
        wal_retention_period = 3600

        stbName = "meters"
        stbCreateSql=f"CREATE STABLE IF NOT EXISTS {dbName}.{stbName} (ts TIMESTAMP, c1 BOOL, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 binary(255), c9 TIMESTAMP, c10 NCHAR(255), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED, c15 VARBINARY(255), c16 DECIMAL(38, 10), c17 VARCHAR(255), c18 GEOMETRY(10240), c19 DECIMAL(18, 4)) tags(t1 JSON)"

        topicName = "topic_meters"
        topicCreateSql = f"CREATE TOPIC IF NOT EXISTS {topicName} AS SELECT ts, tbname, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19 FROM {stbName}"

        ctbName = "d1001"
        insertSql = f"INSERT INTO {dbName}.{ctbName} USING {dbName}.{stbName} TAGS('{{\"k1\": \"v1\"}}') VALUES (NOW, true, -79, 25761, -83885, 7865351, 3848271.756357, 92575.506626, '8.0742e+19', 752424273771827, '3.082946351e+18', 57, 21219, 627629871, 84394301683266985, '-2.653889251096953e+18', -262609547807621769.7285797, '-7.694200485148515e+19', 'POINT(1.0 1.0)', 57823334285922.827)"
        insertSqls = [insertSql]

        self.mqttConf = {
            'user': "root",
            'passwd': "taosdata",
            'host': "127.0.0.1",
            'port': 6083,
            'qos': 2,
            'topic': "$share/g1/topic_meters",
            'loop_time': .1,
            'loop_count': 300,
            }
        
        self.mqttConf['sub_prop'] = p.Properties(pt.PacketTypes.SUBSCRIBE)
        self.mqttConf['sub_prop'].UserProperty = ('sub-offset', 'earliest')
        self.mqttConf['sub_prop'].UserProperty = ('proto', 'fbv')
        # ---- global parameters end ----#

        tdLog.info(f"basic test 1")
        tdMqtt.dropAllTopicsDbsAndBnodes()
        tdMqtt.createBnode()
        tdMqtt.createBnode(2)
        tdSql.query("show bnodes")
        tdSql.checkKeyExist(2)

        dnode_idx = 3
        tdSql.error(f"create bnode on dnode {dnode_idx}")
        dnode_idx = 0
        tdSql.error(f"create bnode on dnode {dnode_idx}")
        # tdSql.checkKeyExist(dnode_idx)

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname=dbName, vgroups=vgroups, precision=precision, wal_retention_period= wal_retention_period)

        tdLog.info(f"=============== create super table")
        tdSql.execute(stbCreateSql)
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create topic")
        tdSql.execute(topicCreateSql)
        tdLog.info(f"== show topics")
        tdSql.query(f"show topics")
        tdSql.checkRows(1)

        tdLog.info(f"=============== write query data")
        tdSql.executes(insertSqls)
        tdSql.query(f"show tables")
        tdSql.checkKeyExist(ctbName)
        tdSql.query(f"select c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c17,c18 from {dbName}.{ctbName}")
        # tdSql.query(f"select * from {dbName}.{ctbName}")
        tdSql.printResult()

        tdLog.info(f"=============== check insert result")
        insertResultSql = f"select c1, c2 from {dbName}.{ctbName}"
        tdSql.checkResultsByFunc(f"show stables", lambda: tdSql.getRows() == 1)
        tdSql.checkResultsByFunc(
            sql=insertResultSql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, True)
            and tdSql.compareData(0, 1, -79)
        )

        #subscribe and check result
        subMsg = tdMqtt.subscribe(self.mqttConf)
        tdMqtt.checkQos(1)
        tdMqtt.checkRows(1)

        tdMqtt.checkEqual(subMsg['qos'], 1)
