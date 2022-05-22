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

import random
import string
from util.sql import tdSql
from util.dnodes import tdDnodes
import requests
import time
import socket
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
                tdSql.execute(f'drop table if exists {stb}')
            elif type == "restful":
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

tdCom = TDCom()
