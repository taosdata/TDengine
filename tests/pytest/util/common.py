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
import requests
import time
import socket

import taos
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

class TDCom:
    def init(self, conn, logSql):
        tdSql.init(conn.cursor(), logSql)

    def preDefine(self):
        header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        sql_url = "http://127.0.0.1:6041/rest/sql"
        sqlt_url = "http://127.0.0.1:6041/rest/sqlt"
        sqlutc_url = "http://127.0.0.1:6041/rest/sqlutc"
        influx_url = "http://127.0.0.1:6041/influxdb/v1/write"
        telnet_url = "http://127.0.0.1:6041/opentsdb/v1/put/telnet"
        return header, sql_url, sqlt_url, sqlutc_url, influx_url, telnet_url

    def genTcpParam(self):
        MaxBytes = 1024*1024
        host ='127.0.0.1'
        port = 6046
        return MaxBytes, host, port

    def tcpClient(self, input):
        MaxBytes = tdCom.genTcpParam()[0]
        host = tdCom.genTcpParam()[1]
        port = tdCom.genTcpParam()[2]
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.send(input.encode())
        sock.close()

    def restApiPost(self, sql):
        requests.post(self.preDefine()[1], sql.encode("utf-8"), headers = self.preDefine()[0])

    def createDb(self, dbname="test", db_update_tag=0, api_type="taosc"):
        if api_type == "taosc":
            if db_update_tag == 0:
                tdSql.execute(f"drop database if exists {dbname}")
                tdSql.execute(f"create database if not exists {dbname} precision 'us'")
            else:
                tdSql.execute(f"drop database if exists {dbname}")
                tdSql.execute(f"create database if not exists {dbname} precision 'us' update 1")
        elif api_type == "restful":
            if db_update_tag == 0:
                self.restApiPost(f"drop database if exists {dbname}")
                self.restApiPost(f"create database if not exists {dbname} precision 'us'")
            else:
                self.restApiPost(f"drop database if exists {dbname}")
                self.restApiPost(f"create database if not exists {dbname} precision 'us' update 1")
        tdSql.execute(f'use {dbname}')

    def genUrl(self, url_type, dbname, precision):
        if url_type == "influxdb":
            if precision is None:
                url = self.preDefine()[4] + "?" + "db=" + dbname
            else:
                url = self.preDefine()[4] + "?" + "db=" + dbname + "&precision=" + precision
        elif url_type == "telnet":
            url = self.preDefine()[5] + "/" + dbname
        else:
            url = self.preDefine()[1]
        return url

    def schemalessApiPost(self, sql, url_type="influxdb", dbname="test", precision=None):
        if url_type == "influxdb":
            url = self.genUrl(url_type, dbname, precision)
        elif url_type == "telnet":
            url = self.genUrl(url_type, dbname, precision)
        res = requests.post(url, sql.encode("utf-8"), headers = self.preDefine()[0])
        return res

    def cleanTb(self, type="taosc"):
        '''
            type is taosc or restful
        '''
        query_sql = "show stables"
        res_row_list = tdSql.query(query_sql, True)
        stb_list = map(lambda x: x[0], res_row_list)
        for stb in stb_list:
            if type == "taosc":
                tdSql.execute(f'drop table if exists `{stb}`')
                if not stb[0].isdigit():
                    tdSql.execute(f'drop table if exists {stb}')
            elif type == "restful":
                self.restApiPost(f"drop table if exists `{stb}`")
                if not stb[0].isdigit():
                    self.restApiPost(f"drop table if exists {stb}")

    def dateToTs(self, datetime_input):
        return int(time.mktime(time.strptime(datetime_input, "%Y-%m-%d %H:%M:%S.%f")))

    def getLongName(self, len, mode = "mixed"):
        """
            generate long name
            mode could be numbers/letters/letters_mixed/mixed
        """    
        if mode == "numbers": 
            chars = ''.join(random.choice(string.digits) for i in range(len))
        elif mode == "letters": 
            chars = ''.join(random.choice(string.ascii_letters.lower()) for i in range(len))
        elif mode == "letters_mixed": 
            chars = ''.join(random.choice(string.ascii_letters.upper() + string.ascii_letters.lower()) for i in range(len))
        else:
            chars = ''.join(random.choice(string.ascii_letters.lower() + string.digits) for i in range(len))
        return chars

    def restartTaosd(self, index=1, db_name="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use {db_name}")

    def typeof(self, variate):
        v_type=None
        if type(variate) is int:
            v_type = "int"
        elif type(variate) is str:
            v_type = "str"
        elif type(variate) is float:
            v_type = "float"
        elif type(variate) is bool:
            v_type = "bool"
        elif type(variate) is list:
            v_type = "list"
        elif type(variate) is tuple:
            v_type = "tuple"
        elif type(variate) is dict:
            v_type = "dict"
        elif type(variate) is set:
            v_type = "set"
        return v_type

    def splitNumLetter(self, input_mix_str):
        nums, letters = "", ""
        for i in input_mix_str:
            if i.isdigit():
                nums += i
            elif i.isspace():
                pass
            else:
                letters += i
        return nums, letters

    def smlPass(self, func):
        smlChildTableName = "no"
        def wrapper(*args):
            # if tdSql.getVariable("smlChildTableName")[0].upper() == "ID":
            if smlChildTableName.upper() == "ID":
                return func(*args)
            else:
                pass
        return wrapper

    def close(self):
        self.cursor.close()

    def create_database(self,tsql, dbName='test',dropFlag=1,precision="ms", **kwargs):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))
        '''
        vgroups replica precision strict wal fsync comp cachelast single_stable buffer pagesize pages minrows maxrows duration keep retentions
        '''
        sqlString = f'create database if not exists {dbName} precision "{precision}" vgroups 4'
        if len(kwargs) > 0:
            dbParams = ""
            for param, value in kwargs.items():
                dbParams += f'{param} {value} '
            sqlString += f'{dbParams}'

        tsql.execute(sqlString)
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tsql, dbName,stbName,columnDict,tagDict):
        colSchema = ''
        for i in range(columnDict['int']):
            colSchema += ', c%d int'%i
        tagSchema = ''
        for i in range(tagDict['int']):
            if i > 0:
                tagSchema += ','
            tagSchema += 't%d int'%i

        tsql.execute("create table if not exists %s.%s (ts timestamp %s) tags(%s)"%(dbName, stbName, colSchema, tagSchema))
        tdLog.debug("complete to create %s.%s" %(dbName, stbName))
        return

    def create_ctables(self,tsql, dbName,stbName,ctbNum,tagDict):
        tsql.execute("use %s" %dbName)        
        tagsValues = ''
        for i in range(tagDict['int']):
            if i > 0:
                tagsValues += ','
            tagsValues += '%d'%i

        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            sql += " %s_%d using %s tags(%s)"%(stbName,i,stbName,tagsValues)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)
        
        tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
        return

    def insert_data(self,tsql,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert
        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            sql += " %s_%d values "%(stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, %d)"%(startTs + j, j, j)
                if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s_%d values " %(stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return
        
    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath        

    def getClientCfgPath(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = buildPath + "/../sim/psim/cfg"
        tdLog.info("cfgPath: %s" % cfgPath)
        return cfgPath

    def newcur(self,host='localhost',port=6030,user='root',password='taosdata'):
        cfgPath = self.getClientCfgPath()
        con=taos.connect(host=host, user=user, password=password, config=cfgPath, port=port)
        cur=con.cursor()
        print(cur)
        return cur

    def newTdSql(self, host='localhost',port=6030,user='root',password='taosdata'):
        newTdSql = TDSql()
        cur = self.newcur(host=host,port=port,user=user,password=password)
        newTdSql.init(cur, False)
        return newTdSql

tdCom = TDCom()
