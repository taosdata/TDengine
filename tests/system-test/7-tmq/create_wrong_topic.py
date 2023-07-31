import taos
import sys
import time
import socket
import os
import threading
from util.common import *

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.sqlset import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(),logSql)
        self.setsql = TDSetSql()
        self.rowNum = 10
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.column_dict = {
            'ts'  : 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            }
        self.error_topic = ['avg','count','spread','stddev','sum','hyperloglog']
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def wrong_topic(self):
        tdSql.prepare()
        tdSql.execute('use db')
        stbname = f'db.{tdCom.getLongName(5, "letters")}'
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,self.column_dict,tag_dict))
        tdSql.execute(f"create table {stbname}_tb1 using {stbname} tags({tag_values[0]})")
        self.insert_data(self.column_dict,f'{stbname}_tb1',self.rowNum)
        for column in self.column_dict.keys():
            for func in self.error_topic:
                if func.lower() != 'count' and column.lower() != 'ts':
                    tdSql.error(f'create topic tpn as select {func}({column}) from {stbname}')
                elif func.lower() == 'count' :
                    tdSql.error(f'create topic tpn as select {func}(*) from {stbname}')
        for column in self.column_dict.keys():
            if column.lower() != 'ts':
                tdSql.error(f'create topic tpn as select apercentile({column},50) from {stbname}')
                tdSql.error(f'create topic tpn as select leastquares({column},1,1) from {stbname}_tb1')
                tdSql.error(f'create topic tpn as select HISTOGRAM({column},user_input,[1,3,5,7],0) from {stbname}')
                tdSql.error(f'create topic tpn as select percentile({column},1) from {stbname}_tb1')
        pass
    def run(self):
        self.wrong_topic()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
