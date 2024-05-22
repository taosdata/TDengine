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

    def check_create_normal_table_result(self):
        createSql = "create table %s.t1(ts timestamp, c1 int, c2 json template '%s',c3 json template '%s')" % (self.dbname, self.jsonTemplate, self.jsonTemplate1)
        print(createSql)
        tdSql.execute(createSql)
        tdSql.query(f'desc {self.dbname}.t1')
        tdSql.checkRows(4)
        tdSql.checkData(0, 7, '')
        tdSql.checkData(1, 7, '')
        tdSql.checkData(2, 7, '1:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"}')
        tdSql.checkData(3, 7, '1:{"k1":"string","k2":"long"}')

        alterSql = '''alter table t1 modify column c2 add template '{"k1":"string","k2":"boolean"}' '''
        tdSql.execute(alterSql)

        tdSql.query(f'desc {self.dbname}.t1')
        tdSql.checkData(2, 7, '1:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"},2:{"k1":"string","k2":"boolean"}')

        tdSql.error('''alter table t1 modify column c2 add template '{"k1":"string","k2":"boolean"}' ''')
        tdSql.error('''alter table t1 modify column c2 add template '{"k1":"string","k2":"booean"}' ''')
        tdSql.error('''alter table t1 modify column c2 add template 'fasd' ''')
        tdSql.error('''alter table t1 modify column c2 add template 4 ''')

        tdSql.error('''alter table t1 modify column c2 drop template "1" ''')

        alterSql = '''alter table t1 modify column c2 drop template 2'''
        tdSql.execute(alterSql)

        tdSql.query(f'desc {self.dbname}.t1')
        tdSql.checkData(2, 7, '1:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"}')

        alterSql = '''alter table t1 modify column c2 add template '{"k1":"string","k2":"boolean"}' '''
        tdSql.execute(alterSql)

        tdSql.query(f'desc {self.dbname}.t1')
        tdSql.checkData(2, 7, '1:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"},3:{"k1":"string","k2":"boolean"}')

    def check_create_super_table_result(self):
        createSql = "create table %s.t2(ts timestamp, c1 int, c2 json template '%s',c3 json template '%s') tags(t int)" % (self.dbname, self.jsonTemplate, self.jsonTemplate1)
        print(createSql)
        tdSql.execute(createSql)
        tdSql.query(f'desc {self.dbname}.t2')
        tdSql.checkRows(5)
        tdSql.checkData(0, 7, '')
        tdSql.checkData(1, 7, '')
        tdSql.checkData(2, 7, '1:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"}')
        tdSql.checkData(3, 7, '1:{"k1":"string","k2":"long"}')
        tdSql.checkData(4, 7, '')

        alterSql = '''alter table t2 modify column c2 add template '{"k1":"string","k2":"boolean"}' '''
        tdSql.execute(alterSql)

        tdSql.query(f'desc {self.dbname}.t2')
        tdSql.checkData(2, 7, '1:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"},2:{"k1":"string","k2":"boolean"}')

        tdSql.error('''alter table t2 modify column c2 add template '{"k1":"string","k2":"boolean"}' ''')
        tdSql.error('''alter table t2 modify column c2 add template '{"k1":"string","k2":"booean"}' ''')
        tdSql.error('''alter table t2 modify column c2 add template 'fasd' ''')
        tdSql.error('''alter table t2 modify column c2 add template 4 ''')

        tdSql.error('''alter table t2 modify column c2 drop template "1" ''')

        alterSql = '''alter table t2 modify column c2 drop template 2'''
        tdSql.execute(alterSql)

        tdSql.query(f'desc {self.dbname}.t2')
        tdSql.checkData(2, 7, '1:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"}')

        alterSql = '''alter table t2 modify column c2 add template '{"k1":"string","k2":"boolean"}' '''
        tdSql.execute(alterSql)

        tdSql.query(f'desc {self.dbname}.t2')
        tdSql.checkData(2, 7, '1:{"k1":"string","k2":"long","k3":["double"],"k4":"long","k5":{"k6":"boolean","k7":"double"},"k8":"string"},3:{"k1":"string","k2":"boolean"}')

    def run(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')

        self.check_create_normal_table_result()
        self.check_create_super_table_result()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
