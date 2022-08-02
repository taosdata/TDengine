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

# from taostest import TDCase, T
# from taostest.util.sql import TDSql
# from taostest.util.common import TDCom
# from taostest.util.tmqCommon import TMQCom
# from taostest.util.remote import Remote

from .tmqCommon import *

import socket
import os
import threading
import random
import datetime
import re
import time
import json

class TestTmq(TDCase):
    def init(self):
        self.tmqCom = TMQCom(self.tdSql, self.logger)
        
    # parse config file
    def parse_config_file(self):
        # read config file and generate config dict
        config_dict = dict()
        lines = []
        with open(self.config_file, 'r') as file: 
            lines = file.readlines()
        for line in lines:
            line_stripped = line.strip()
            self.logger.debug(" {}".format(line_stripped))
            if line_stripped == "":
                continue
            if line_stripped.startswith("#"):
                continue
            pos = line_stripped.find("=")
            if pos <= 0:
                continue
            key = line_stripped[0:pos]
            value = line_stripped[pos+1:]
            config_dict[key] = value
        self.logger.debug(str(config_dict))
        return config_dict

    # create tmp dir
    def make_tmp_dir(self):
        self.logger.debug("log dir: {}".format(self.run_log_dir))
        tmp_dir = os.path.join(self.run_log_dir, "tmp")
        os.system("mkdir -p {}".format(tmp_dir))
        return tmp_dir

    # replace configuration
    def replace_config(self, filename, config):
        for key, value in config.items():
            os.system("sed -i \"s/\<{}\>/{}/g\" {}".format(key, value, filename))
                            
    def getInsertInfo(self, json_file):
        rowsOfStbList = []
        benchmark_config = dict()
        with open(json_file, 'r') as file: 
            benchmark_config = json.load(file)
        for db in benchmark_config["databases"]:
            db_name = db["dbinfo"]["name"]
            self.logger.debug("db_name: {}".format(db_name))
            for stb in db["super_tables"]:
                stb_name = stb["name"]
                childtable_count = stb["childtable_count"]
                insert_rows = stb["insert_rows"]
                self.logger.debug("stb_name: {}".format(stb_name))
                self.logger.debug("childtable_count: {}".format(childtable_count))
                self.logger.debug("insert_rows: {}".format(insert_rows))
                rowsOfStbList.append(childtable_count * insert_rows)
        return rowsOfStbList

    def run(self) -> bool:
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'vgroups':    1,
                    'stbName':    'stbq',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctbq',
                    'ctbStartIdx': 0,
                    'ctbNum':     1000,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0,
                    'processorName': 'tmqSim',
                    'json_template_filename':'',
                    'host': 'localhost',
                    'cfgPath': '/data/testframe/cfg'}        
        
        self.logger.info("CONFIG FILE: %s", self.config_file)
        
        if (self.case_param is None):
            self.logger.error("case_param not specified in taostest args")
        
        # read config file and generate config dict
        config_dict = self.parse_config_file()
        
        self.logger.info("==== config item:")
        self.logger.info(config_dict)

        # create tmp dir
        tmp_dir = self.make_tmp_dir()
        json_template_filename = os.path.basename(self.case_param)
        self.logger.debug("json template basename: {}".format(json_template_filename))
        json_file = os.path.join(tmp_dir, json_template_filename)
        self.logger.debug("json file: {}".format(json_file))
        # copy json template to a tmp directory        
        os.system("cp -f {}/{} {}".format(os.environ["TEST_ROOT"], self.case_param, json_file))

        # get taosd host and port
        taosd_nodes = self.get_component_by_name("taosd")
        self.logger.debug(str(taosd_nodes))
        for node in taosd_nodes:
            if (not node["spec"] is None) and (not node["spec"]["config"] is None) and (not node["spec"]["config"]["firstEP"] is None):
                host = node["spec"]["config"]["firstEP"].split(":")[0]
                port = node["spec"]["config"]["firstEP"].split(":")[1]
                self.logger.debug("{} : {}".format(host, port))
                config_dict["HOST"] = host
                config_dict["PORT"] = port
                break
        if (not "HOST" in config_dict) or (not "PORT" in config_dict):
            self.logger.error("firstEP not specified in env file")

        # replace config settings in json template
        self.replace_config(json_file, config_dict)
        os.system("cat {}".format(json_file))

        # put json file to host
        # self.tmqCom._remote.put()
        self.envMgr._remote.put(config_dict["HOST"], json_file, "/tmp")
        
        rowsOfStbList = self.getInsertInfo(json_file)
        totalInsertRows = 0
        for i in range(len(rowsOfStbList)):
            totalInsertRows += rowsOfStbList[i]

        # create database, stb, and create topic
        self.tmqCom.initConsumerTable()
        
        self.tdSql.query("drop database if exists %s"%(paraDict['dbName']))
        time.sleep(2)
        self.tdSql.query("create database %s"%(paraDict['dbName']))
        
        schemaString = "(ts timestamp, c1 int, c2 bigint, c3 double, c4 binary(32), c5 nchar(32), c6 timestamp) tags (t1 int, t2 bigint, t3 double, t4 binary(32), t5 nchar(32))"
        self.tdSql.query("create table %s.%s %s"%(paraDict['dbName'],paraDict['stbName'],schemaString))
        
        topicName = 'tmqtopic'
        # topicQuerySql = "select ts, acos(c1), ceil(pow(c1,3)) from %s.%s where (sin(c2) >= 0) and (c1 %% 4 == 0) "%(paraDict['dbName'], paraDict['stbName'])
        topicQuerySql = "select ts, c6, acos(c1), ceil(pow(c1,3)) from %s.%s "%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicName, topicQuerySql)
        self.logger.info("create topic sql: %s"%sqlString)
        self.tdSql.query(sqlString)        
        # self.tdSql.query("show topics")        
        # self.logger.info(self.tdSql.getData(0,0))        

        # run benchmark
        self.logger.info("run taosBenchmark")
        paraDict["host"] = config_dict["HOST"]
        paraDict['json_template_filename'] = json_template_filename
        paraDict['processorName'] = 'taosBenchmark'
        pThreadOfTaosBenchmark = self.tmqCom.startOtherProcessor(paraDict)
        
        # pThreadOfTaosBenchmark.join()
        
        
        time.sleep(2)

        # init consume info, and start tmq_sim
        self.logger.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = totalInsertRows
        topicList    = topicName
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:1000, auto.offset.reset:earliest'
        self.tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        self.logger.info("run tmq_sim")
        paraDict['processorName'] = 'tmq_sim'
        pThreadOfTmqSim = self.tmqCom.startOtherProcessor(paraDict)            
        
        self.logger.info("wait processer return")
        pThreadOfTaosBenchmark.join()    
        pThreadOfTmqSim.join()
        
        self.logger.info("================= result ======================")         
        
           
        self.tdSql.query("select count(tbname) from {}.{};".format(paraDict['dbName'], paraDict['stbName']))
        # self.logger.info(self.tdSql.query_result)
        totalTables = self.tdSql.getData(0,0)
        self.logger.info("total tables: %d"%totalTables)
        self.tdSql.query("select count(*) from {}.{};".format(paraDict['dbName'], paraDict['stbName']))
        totalRows = self.tdSql.getData(0,0)
        self.logger.info("total rows: %d"%totalRows)
        
        self.tdSql.query(topicQuerySql)
        expectConsumeRows = self.tdSql.query_row
        
        expectRows = 1
        resultList = self.tmqCom.selectConsumeResult(expectRows)        
        
        self.logger.info("expect consume rows: %d, act consume rows: %d"%(expectConsumeRows, resultList[0]))
        
        if expectConsumeRows != resultList[0]:
            self.logger.info("expect consume rows: %d, act consume rows: %d"%(expectConsumeRows, resultList[0]))
            self.logger.error("%d tmq consume rows error!"%consumerId)
            return False
        

        return True

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            stability test for tmq feature
        """
        return case_description

    def author(self) -> str:
        return "lihui"

    def tags(self):
        return ""

