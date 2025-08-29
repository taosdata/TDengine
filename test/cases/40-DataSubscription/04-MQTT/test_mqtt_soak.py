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
from new_test_framework.utils import tdLog, tdSql, tdMqtt, sc, clusterComCheck, etool

import paho.mqtt.properties as p
import paho.mqtt.packettypes as pt


class TestMqttCases:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mqtt_soak(self):
        """MQTT: soak testing

        Soak testing for 72 hours to assess taosmqtt's behavior under prolonged stress.
        -N [6]

        Catalog:
            - Subscribe:Mqtt

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-27 Created by stephenkgu

        """

        tdLog.debug(f"start to excute {__file__}")
        self.soak()

    def soak(self):
        self.vgroups = 64
        self.sub_vgroup(self.vgroups)

        print("sleeping 20s for debugging")
        # time.sleep(20)
        print("wake up")

    def sub_vgroup(self, vgroups=1):
        # ---- global parameters start ----#
        dbName = "power"
        precision = "ms"
        wal_retention_period = 5*24*3600

        stbName = "meters"
        stbCreateSql=f"CREATE STABLE IF NOT EXISTS {dbName}.{stbName} (ts TIMESTAMP, c1 BOOL, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 binary(255), c9 TIMESTAMP, c10 NCHAR(255), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED, c15 VARBINARY(255), c17 VARCHAR(255), c18 GEOMETRY(10240)) tags(t1 JSON)"
        #stbCreateSql=f"CREATE STABLE IF NOT EXISTS {dbName}.{stbName} (ts TIMESTAMP, c1 BOOL, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 binary(255), c9 TIMESTAMP, c10 NCHAR(255), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED, c15 VARBINARY(255), c16 DECIMAL(38, 10), c17 VARCHAR(255), c18 GEOMETRY(10240), c19 DECIMAL(18, 4)) tags(t1 JSON)"

        topicNames = [
            "topic_meters",
            "topic_meters_stb",
            "topic_meters_db",
        ]
        
        topicCreateSqls = [
            #f"CREATE TOPIC IF NOT EXISTS {topicNames[0]} AS SELECT ts, tbname, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19 FROM {stbName}",
            f"CREATE TOPIC IF NOT EXISTS {topicNames[0]} AS SELECT ts, tbname, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c17, c18 FROM {stbName}",
            f"create topic if not exists {topicNames[1]} with meta as stable {stbName}",
            f"create topic if not exists {topicNames[2]} with meta as database {dbName}",
        ]

        #ctbName = "d1001"
        #insertSql = f"INSERT INTO {dbName}.{ctbName} USING {dbName}.{stbName} TAGS('{{\"k1\": \"v1\"}}') VALUES (NOW, true, -79, 25761, -83885, 7865351, 3848271.756357, 92575.506626, '8.0742e+19', 752424273771827, '3.082946351e+18', 57, 21219, 627629871, 84394301683266985, '-2.653889251096953e+18', -262609547807621769.7285797, '-7.694200485148515e+19', 'POINT(1.0 1.0)', 57823334285922.827)"
        #insertSqls = [insertSql]

        stb_rows = 30
        self.mqttConf = {
            'user': "root",
            'passwd': "taosdata",
            'host': "127.0.0.1",
            'port': 6083,
            'qos': 2,
            'topic': "$share/g1/topic_meters",
            'loop_time': .1,
            'loop_count': 3*24*60*60*10,
            'rows': stb_rows,
            'client_id': 0,
            }
        
        self.mqttConf['sub_prop'] = p.Properties(pt.PacketTypes.SUBSCRIBE)
        self.mqttConf['sub_prop'].UserProperty = ('sub-offset', 'earliest')
        self.mqttConf['sub_prop'].UserProperty = ('proto', 'fbv')
        self.mqttConf['conn_prop'] = p.Properties(pt.PacketTypes.CONNECT)
        self.mqttConf['conn_prop'].SessionExpiryInterval = 60
        # ---- global parameters end ----#

        # 1, create 6 bnodes on 6 dnodes
        tdLog.info(f"test bnode creating")

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

        # 2, create db & stable, 4 query topics, 4 stable, 4 db ones
        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname=dbName, vgroups=vgroups, precision=precision, wal_retention_period= wal_retention_period)

        tdLog.info(f"=============== create super table")
        tdSql.execute(stbCreateSql)
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create topic")
        for topicCreateSql in topicCreateSqls:
            tdSql.execute(topicCreateSql)
        tdLog.info(f"== show topics")
        tdSql.query(f"show topics")
        tdSql.checkRows(3)

        print(f"4 {self.mqttConf['port']}")
        # 3, 5 async sub clients for each bnode
        tdLog.info(f"=============== start data subscribing")
        topics = [
            topicNames[0],
            topicNames[0],
            topicNames[0],
            topicNames[1],
            topicNames[2],
        ]
        qos = [
            0,
            1,
            2,
            0,
            0,
        ]
        
        for i in range(dnodes_count):
        #for i in range(1):
            for ci in range(len(topics)):
                gid = "g" + str(ci)
                topic = topics[ci]
                conf = {
                    'user': "root",
                    'passwd': "taosdata",
                    'host': "127.0.0.1",
                    'port': self.mqttConf['port'] + i * 100,
                    'qos': qos[ci],
                    'topic': f'$share/{gid}/{topic}',
                    'loop_time': .1,
                    'loop_count': 3*24*60*60*10,
                    'rows': stb_rows,
                    'client_id': ci,
                    'sub_prop': self.mqttConf['sub_prop'],
                    'conn_prop': self.mqttConf['conn_prop'],
                }

                print(f"4 {i} {conf['port']} {conf['topic']}")
                tdMqtt.asyncSubscribe(conf)

        # 4, launch benchmark to start table creating and data inserting
        tdLog.info(f"=============== write sub data")
        self.insertData()
        #self.doAction()
        
        tdSql.query(f"select c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c17,c18 from {dbName}.{stbName}")
        tdSql.checkRows(stb_rows)
        tdLog.sleep(20000)

    def drop_bnodes_dbs(self):
        tdSql.query("show bnodes")
        self.bnodes_count = tdSql.getRows()
        tdSql.checkKeyExist(self.bnodes_count)
        for i in range(self.bnodes_count):
            tdMqtt.dropBnode(i+1);

        tdMqtt.dropAllTopicsDbsAndBnodes()

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "soak.json")
        etool.benchMark(json=json)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 4
        self.insert_rows = 1000000
        self.timestamp_step = 1000

    def doAction(self):
        tdLog.info(f"do action.")
        self.flushDb()
        self.trimDb()
        self.compactDb()
