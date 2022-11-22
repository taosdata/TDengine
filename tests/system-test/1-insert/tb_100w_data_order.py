from util.log import *
from util.cases import *
from util.sql import *
from util.common import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'

    def set_create_normaltable_sql(self, ntbname, column_dict):
        column_sql = ''
        for k, v in column_dict.items():
            column_sql += f"{k} {v},"
        create_ntb_sql = f'create table {ntbname} (ts timestamp,{column_sql[:-1]})'
        return create_ntb_sql

    def set_create_stable_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v},"
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v},"
        create_stb_sql = f'create table {stbname} (ts timestamp,{column_sql[:-1]}) tags({tag_sql[:-1]})'
        return create_stb_sql

    def gen_batch_sql(self, ntbname, batch=10):
        values_str = ""
        for i in range(batch):
            values_str += f'({self.ts}, 1, 1, 1, {i+1}, 1, 1, 1, {i+1}, {i+0.1}, {i+0.1}, {i%2}, {i+1}, {i+1}),'
            self.ts += 1
        return f'insert into {ntbname} values {values_str[:-1]};'

    def query_ntb_order_by_col(self, batch_num, rows_count):
        tdSql.prepare()
        ntbname = f'db.{tdCom.getLongName(5, "letters")}'
        column_dict = {
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
            'col11': 'bool',
            'col12': 'binary(20)',
            'col13': 'nchar(20)'
        }
        range_times = int(rows_count/batch_num)
        create_ntb_sql = self.set_create_normaltable_sql(ntbname, column_dict)
        tdSql.execute(create_ntb_sql)
        for i in range(range_times):
            tdSql.execute(self.gen_batch_sql(ntbname, batch_num))
        tdSql.query(f'select count(*) from {ntbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0], rows_count)
        tdSql.query(f'select * from {ntbname} order by col1')
        tdSql.execute(f'flush database db')


    def run(self):
        self.query_ntb_order_by_col(batch_num=1000, rows_count=1000000)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
