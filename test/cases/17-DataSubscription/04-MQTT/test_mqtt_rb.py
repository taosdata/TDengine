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

import paho.mqtt.properties as p
import paho.mqtt.packettypes as pt


class TestMqttCases:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mqtt_rawblock(self):
        """MQTT: rawblock test

        mqtt rawblock testing

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-27 Created by stephenkgu

        """

        self.rawblock()

    def rawblock(self):
        self.sub_vgroup()

        # time.sleep(20)

    def sub_vgroup(self, vgroups=1):
        # ---- global parameters start ----#
        dbName = "power"
        precision = "us"
        wal_retention_period = 3600

        stbName = "meters"
        # stbCreateSql=f"CREATE STABLE IF NOT EXISTS {dbName}.{stbName} (ts TIMESTAMP, c1 BOOL, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 binary(255), c9 TIMESTAMP, c10 NCHAR(255), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED, c15 VARBINARY(255), c16 DECIMAL(38, 10), c17 VARCHAR(255), c18 GEOMETRY(10240), c19 DECIMAL(18, 4)) tags(t1 JSON)"
        stbCreateSql=f"CREATE STABLE IF NOT EXISTS {dbName}.{stbName} (ts TIMESTAMP, c1 BOOL, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 binary(255), c9 TIMESTAMP, c10 NCHAR(255), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED, c15 VARBINARY(255), c17 VARCHAR(255)) tags(t1 JSON)"

        topicName = "topic_meters"
        # topicCreateSql = f"CREATE TOPIC IF NOT EXISTS {topicName} AS SELECT ts, tbname, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19 FROM {stbName}"
        topicCreateSql = f"CREATE TOPIC IF NOT EXISTS {topicName} AS SELECT ts, tbname, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c17 FROM {stbName}"

        ctbName = "d1001"
        insertSql = f"INSERT INTO {dbName}.{ctbName} USING {dbName}.{stbName} TAGS('{{\"k1\": \"v1\"}}') VALUES (NOW, true, -79, 25761, -83885, 7865351, 3848271.756357, 92575.506626, '8.0742e+19', 752424273771827, '3.082946351e+18', 57, 21219, 627629871, 84394301683266985, '-2.653889251096953e+18', '-7.694200485148515e+19')"
        insertSqls = [insertSql]

        stb_rows = 30
        self.mqttConf = {
            'user': "root",
            'passwd': "taosdata",
            'host': "127.0.0.1",
            'port': 6083,
            'qos': 2,
            'topic': "$share/g1/topic_meters",
            'loop_time': .1,
            'loop_count': 300,
            'rows': stb_rows,
            }
        
        self.mqttConf['sub_prop'] = p.Properties(pt.PacketTypes.SUBSCRIBE)
        self.mqttConf['sub_prop'].UserProperty = ('sub-offset', 'earliest')
        self.mqttConf['sub_prop'].UserProperty = ('proto', 'rawblock')
            
        self.mqttConf['conn_prop'] = p.Properties(pt.PacketTypes.CONNECT)
        self.mqttConf['conn_prop'].SessionExpiryInterval = 60
        # ---- global parameters end ----#

        tdLog.info(f"test topic creating")

        tdMqtt.dropAllTopicsDbsAndBnodes()

        tdSql.query("show dnodes")
        dnodes_count = tdSql.getRows()
        print(f"dnode count: {dnodes_count}")

        for dnode_index in range(1, dnodes_count + 1):
            print(f"create bnode {dnode_index}")
            tdMqtt.createBnode(dnode_index)

        tdSql.query("show bnodes")
        for dnode_index in range(1, dnodes_count + 1):
            print(f"check bnode {dnode_index}")
            tdSql.checkKeyExist(dnode_index)

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

        tdLog.info(f"=============== write sub data")
        for i in range(stb_rows):
            ctbName = "d" + str(i)
            ts = 1750150250056200 + i
            insertSql = f"INSERT INTO {dbName}.{ctbName} USING {dbName}.{stbName} TAGS('{{\"k1\": \"v1\"}}') VALUES ({ts}, true, -79, 25761, -83885, 7865351, 3848271.756357, 92575.506626, '8.0742e+19', 752424273771827, '3.082946351e+18', 57, 21219, 627629871, 84394301683266985, '-2.653889251096953e+18', '-7.694200485148515e+19')"
            insertSqls = [insertSql]

            tdSql.executes(insertSqls)

        tdSql.query(f"select c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c17 from {dbName}.{stbName}")
        tdSql.checkRows(stb_rows)

        # qos 2 default to 1
        subMsg = tdMqtt.subscribe(self.mqttConf)
        tdMqtt.checkQos(1)
        tdMqtt.checkEqual(subMsg['qos'], 1)
        subRows = tdMqtt.getRows()
        print(f"sub rows: {subRows}")
        tdMqtt.checkRows(stb_rows)

        self.drop_bnodes_dbs()
        
    def drop_bnodes_dbs(self):
        tdSql.query("show bnodes")
        self.bnodes_count = tdSql.getRows()
        tdSql.checkKeyExist(self.bnodes_count)
        for i in range(self.bnodes_count):
            tdMqtt.dropBnode(i+1)

        tdMqtt.dropAllTopicsDbsAndBnodes()

    def test_node_create_bnode(self):
        """Node create bnode

        1. Create bnode for each dnode
        2. Create database with us precision
        3. Create stable with full data types
        4. Create topic from stable
        5. Insert data into stable
        6. Subscribe topic with rawblock proto
        7. Verify subscribed data
        8. Drop bnodes and databases

        Catalog:
            - NodeManager:Bnode

        Since: v3.3.3.0

        Labels: common,ci

        Jira: None

        """
        pass