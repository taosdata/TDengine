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
from new_test_framework.utils import tdLog, tdSql, tdMqtt

import paho.mqtt
import paho.mqtt.properties as p
import paho.mqtt.packettypes as pt
import paho.mqtt.client as mqttClient


class TestMqttDevBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mqtt_dev_smoke(self):
        """MQTT: development testing

        Verification testing during the development process.

        Catalog:
            - Subscribe:Mqtt

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-27 Created by stephenkgu

        """

        self.smoke()
        # self.basic2()

    def smoke(self):
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
        # ---- global parameters end ----#

        tdLog.info(f"basic test 1")
        tdMqtt.dropAllTopicsDbsAndBnodes()
        tdMqtt.createBnode()

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
        # tdSql.printResult()

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
        if paho.mqtt.__version__[0] > '1':
            client = mqttClient.Client(mqttClient.CallbackAPIVersion.VERSION2, client_id="tmq_sub_client_id3", userdata=None, protocol=mqttClient.MQTTv5)
        else:
            client = mqttClient.Client(client_id="tmq_sub_client_id3", userdata=None, protocol=mqttClient.MQTTv5)

        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe
        client.on_message = self.on_message

        client.username_pw_set("root", "taosdata")
        client.connect("127.0.1.1", 6083)
        client.loop_start()

        self.loop_count = 0
        self.loop_stop = False
        while not self.loop_stop:
            # client.loop(.1)

            if ++self.loop_count > 300: # timeout for 30 seconds
                tdLog.exit("too long to sub msg")
            tdLog.sleep(.1)

        client.loop_stop()

        tdLog.info(f"=============== subscription finished.")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        print("\nCONNACK received with code %s." % rc)
        sub_properties = p.Properties(pt.PacketTypes.SUBSCRIBE)

        sub_properties.UserProperty = ('sub-offset', 'earliest')
        sub_properties.UserProperty = ('proto', 'fbv')

        client.subscribe("$share/g1/topic_meters", qos=2, properties=sub_properties)

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_message(self, client, userdata, msg):
        print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
        
        self.loop_stop = True

    def on_disconnect(client, userdata,rc=0):
        tdLog.debug("DisConnected result code: "+str(rc))
        client.loop_stop()
