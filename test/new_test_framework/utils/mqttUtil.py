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

from collections import defaultdict
import random
import string
import threading
import requests
import time
import json
import taos

from .log import *
from .sql import *
from .server.dnodes import *
from .common import *

import paho.mqtt
import paho.mqtt.properties as p
import paho.mqtt.packettypes as pt
import paho.mqtt.client as mqttClient


class MqttUtil:
    def createBnode(self, index=1):
        sql = f"create bnode on dnode {index}"
        tdSql.execute(sql)

        tdSql.query("show bnodes")
        tdSql.checkKeyExist(index)

    def dropBnode(self, index=1):
        sql = f"drop bnode on dnode {index}"
        tdSql.query(sql)

    def checkMqttStatus(self, topic_name=""):
        return

        for loop in range(60):
            if topic_name == "":
                tdSql.query(f"select * from information_schema.ins_topic_tasks")
                if tdSql.getRows() == 0:
                    continue
                tdSql.query(
                    f'select * from information_schema.ins_topic_tasks where status != "ready"'
                )
                if tdSql.getRows() == 0:
                    return
            else:
                tdSql.query(
                    f'select topic_name, status from information_schema.ins_topic_tasks where topic_name = "{topic_name}" and status == "ready"'
                )
                if tdSql.getRows() == 1:
                    return
            time.sleep(1)

        tdLog.exit(f"topic task status not ready in {loop} seconds")

    def dropAllTopicsDbsAndBnodes(self):
        topicList = tdSql.query("show topics", row_tag=True)
        for r in range(len(topicList)):
            tdSql.execute(f"drop topic {topicList[r][0]}")

        dbList = tdSql.query("show databases", row_tag=True)
        for r in range(len(dbList)):
            if (
                dbList[r][0] != "information_schema"
                and dbList[r][0] != "performance_schema"
            ):
                tdSql.execute(f"drop database {dbList[r][0]}")

        bnodeList = tdSql.query("show bnodes", row_tag=True)
        for r in range(len(bnodeList)):
            self.dropBnode(bnodeList[r][0])

        tdLog.info(f"drop {len(dbList)} databases, {len(topicList)} topics, {len(bnodeList)} bnodes")

    def asyncSubscribe(self, conf):
        self.subRows = 0

        cid = "tmq_sub_client_id" + str(conf['client_id'])
        if paho.mqtt.__version__[0] > '1':
            client = mqttClient.Client(mqttClient.CallbackAPIVersion.VERSION2, client_id=cid, userdata=conf, protocol=mqttClient.MQTTv5)
        else:
            client = mqttClient.Client(client_id=cid, userdata=conf, protocol=mqttClient.MQTTv5)

        client.on_connect = self.on_connect_async
        client.on_disconnect = self.on_disconnect_async
        client.on_subscribe = self.on_subscribe_async
        client.on_message = self.on_message_async

        client.on_log = self.on_log

        client.username_pw_set(conf['user'], conf['passwd'])
        client.connect(conf['host'], conf['port'], properties=conf['conn_prop'], keepalive=60)
        client.loop_start()

        tdLog.info(f"=============== subscription loop started.")

    def on_connect_async(self, client, userdata, flags, rc, properties=None):
        print("\nCONNACK received with code %s." % rc)

        print("sub: ", userdata['topic'])
        client.subscribe(userdata['topic'], qos=userdata['qos'], properties=userdata['sub_prop'])

    def on_subscribe_async(self, client, userdata, mid, granted_qos, properties=None):
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_message_async(self, client, userdata, msg):
        # print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
        print("msg: ", len(msg.payload))

    def on_log(client, userdata, level, buf):
        tdLog.debug("log: ", buf)
        print("log: ", buf)

    def on_disconnect_async(client, userdata, rc=0):
        tdLog.debug("DisConnected result code: "+ str(rc))
        print("DisConnected result code: "+ str(rc))

        client.reconnect()
        #client.loop_stop()

    def subscribe(self, conf):
        self.subRows = 0
        self.mqttConf = conf

        if paho.mqtt.__version__[0] > '1':
            client = mqttClient.Client(mqttClient.CallbackAPIVersion.VERSION2, client_id="tmq_sub_client_id3", userdata=None, protocol=mqttClient.MQTTv5)
        else:
            client = mqttClient.Client(client_id="tmq_sub_client_id3", userdata=None, protocol=mqttClient.MQTTv5)

        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe
        client.on_unsubscribe = self.on_unsubscribe
        client.on_message = self.on_message

        client.username_pw_set(self.mqttConf['user'], self.mqttConf['passwd'])
        client.connect(self.mqttConf['host'], self.mqttConf['port'], properties=conf['conn_prop'], keepalive=60)
        client.loop_start()

        self.loop_count = 0
        self.loop_stop = False
        while not self.loop_stop:
            # client.loop(.1)

            if ++self.loop_count > self.mqttConf['loop_count']: # timeout for 30 seconds
                self.loop_stop = True
                tdLog.exit("too long to sub msg")
                
            tdLog.sleep(self.mqttConf['loop_time'])

        client.loop_stop()

        tdLog.info(f"=============== subscription finished.")

        return self.subMsg

    def on_connect(self, client, userdata, flags, rc, properties=None):
        print("\nCONNACK received with code %s." % rc)

        client.subscribe(self.mqttConf['topic'], qos=self.mqttConf['qos'], properties=self.mqttConf['sub_prop'])

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_unsubscribe(self, client, userdata, mid):
        print("Unsubscribed: " + str(mid))

    def on_message(self, client, userdata, msg):
        # print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

        self.subMsg = {
            'topic': msg.topic,
            'qos': msg.qos,
            'payload': msg.payload,
            }

        jsonMsg = json.loads(str(self.subMsg['payload'], encoding='utf-8'))
        msgRows = len(jsonMsg['rows'])
        self.subRows += msgRows
        print(f"recieved {msgRows} rows")

        self.qos = msg.qos

        if self.subRows >= self.mqttConf['rows']:
            client.unsubscribe(self.mqttConf['topic'])
            tdLog.sleep(self.mqttConf['loop_time'] * 20)

            self.loop_stop = True
        

    def on_disconnect(client, userdata,rc=0):
        tdLog.debug("DisConnected result code: "+str(rc))
        # client.loop_stop()

    def checkQos(self, expectedQos, show=False):
        """
        Checks if the qos fetched by the last subscription matches the expected qos.

        Args:
            expectedQos (int): The expected qos.

        Returns:
            bool: True if the qos matches the expected qos, otherwise it exits the program.

        Raises:
            SystemExit: If the number of qos does not match the expected qos.
        """
        return self.checkEqual(self.qos, expectedQos, show=show)

    def checkRows(self, expectedRows, show=False):
        """
        Checks if the number of rows fetched by the last subscription matches the expected number of rows.

        Args:
            expectedRows (int): The expected number of rows.

        Returns:
            bool: True if the number of rows matches the expected number, otherwise it exits the program.

        Raises:
            SystemExit: If the number of rows does not match the expected number.
        """
        return self.checkEqual(self.subRows, expectedRows, show=show)

    def getRows(self):
        """
        Retrieves the number of rows fetched by the last sub.

        Args:
            None

        Returns:
            int: The number of rows fetched by the last sub.

        Raises:
            None
        """
        return self.subRows
    
    def print_error_frame_info(self, elm, expect_elm, sql="<empty sql>"):
        caller = inspect.getframeinfo(inspect.stack()[1][0])
        print_sql = self.sql if sql is None else sql
        args = (caller.filename, caller.lineno, print_sql, elm, expect_elm)
        # tdLog.info("%s(%d) failed: sql:%s, elm:%s != expect_elm:%s" % args)
        raise Exception("%s(%d) failed: sql:%s, elm:%s != expect_elm:%s" % args)

    def __check_equal(self, elm, expect_elm):
        if elm == expect_elm:
            return True
        if type(elm) in (list, tuple) and type(expect_elm) in (list, tuple):
            if len(elm) != len(expect_elm):
                return False
            if len(elm) == 0:
                return True
            for i in range(len(elm)):
                flag = self.__check_equal(elm[i], expect_elm[i])
                if not flag:
                    return False
            return True
        return False

    def checkEqual(self, elm, expect_elm, show=False):
        """
        Checks if the given element is equal to the expected element.

        Args:
            elm: The element to be checked.
            expect_elm: The expected element to be compared with.

        Returns:
            None

        Raises:
            Exception: If the element does not match the expected element.
        """
        if elm == expect_elm:
            if show:
                tdLog.info(
                    "sql:%s, elm:%s == expect_elm:%s" % (self.sql, elm, expect_elm)
                )
            return True
        if self.__check_equal(elm, expect_elm):
            if show:
                tdLog.info(
                    "sql:%s, elm:%s == expect_elm:%s" % (self.sql, elm, expect_elm)
                )
            return True
        self.print_error_frame_info(elm, expect_elm)

    def checkNotEqual(self, elm, expect_elm):
        """
        Checks if the given element is not equal to the expected element.

        Args:
            elm: The element to be checked.
            expect_elm: The expected element to be compared with.

        Returns:
            None

        Raises:
            Exception: If the element matches the expected element.
        """
        if elm != expect_elm:
            tdLog.info("sql:%s, elm:%s != expect_elm:%s" % (self.sql, elm, expect_elm))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, elm, expect_elm)
            tdLog.info("%s(%d) failed: sql:%s, elm:%s == expect_elm:%s" % args)
            raise Exception


tdMqtt = MqttUtil()
