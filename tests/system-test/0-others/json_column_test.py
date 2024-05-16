import time
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


class TDTestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'clientCfg':clientCfgDict}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.ttl = 5
        self.dbname = "test"
        self.jsonTemplate = '''{
            "k1": "string",
            "k2": "long",
            "k3": ["double"],
            "k4": "long",
            "k5": {
                "k6": "boolean",
                "k7": "double"
                },
            "k8": "string"
        }'''
        self.jsonTemplate1 = '''{
            "k1": "string",
            "k2": "long"
        }'''

    def check_create_table_result(self):
        tdSql.execute(f'create database {self.dbname}')
        createSql = "create table %s.t1(ts timestamp, c1 int, c2 json template '%s',c3 json template '%s')" % (self.dbname, self.jsonTemplate, self.jsonTemplate1)
        print(createSql)
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(createSql)
        tdSql.query(f'desc {self.dbname}.t1')
        tdSql.checkRows(4)
        tdSql.checkData(0, 7, '')
        tdSql.checkData(1, 7, '')
        tdSql.checkData(2, 7, '0:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"}')
        tdSql.checkData(3, 7, '0:{"k1":"string","k2":"long"}')

        alterSql = '''alter table t1 modify column c2 add template '{"k1":"string","k2":"boolean"}' '''
        tdSql.execute(alterSql)

    def run(self):
        self.check_create_table_result()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
