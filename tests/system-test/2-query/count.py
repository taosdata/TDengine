from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(),True)
        self.setsql = TDSetSql()
        self.rowNum = 10
        self.ts = 1537146000000

        dbname = "db"
        self.ntbname = f'{dbname}.ntb'
        self.stbname = f'{dbname}.stb'
        self.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'double',
            'c4':'timestamp'
        }
        self.tag_dict = {
            't0':'int'
        }
        # The number of tag_values should be same as tbnum
        self.tbnum = 2
        self.tag_values = [
            f'10',
            f'100'
        ]
    def query_stb(self,k,stbname,tbnum,rownum):
        tdSql.query(f'select count({k}) from {stbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        tdSql.query(f'select count({k}) from {stbname} where ts <={self.ts+self.rowNum-1}')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        tdSql.query(f'select count({k}) from {stbname} where ts <={self.ts+self.rowNum-2}')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*(rownum-1))
    def query_ctb(self,k,i,stbname,rownum):
        tdSql.query(f'select count({k}) from {stbname}_{i}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        tdSql.query(f'select count({k}) from {stbname}_{i} where ts <={self.ts+self.rowNum-1}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        tdSql.query(f'select count({k}) from {stbname}_{i} where ts <={self.ts+self.rowNum-2}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum-1)
    def query_ntb(self,k,ntbname,rownum):
        tdSql.query(f'select count({k}) from {ntbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        tdSql.query(f'select count({k}) from {ntbname} where ts <={self.ts+self.rowNum-1}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        tdSql.query(f'select count({k}) from {ntbname} where ts <={self.ts+self.rowNum-2}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum-1)
    def query_empty_stb(self):
        tdSql.query(f'select count(*) from (select distinct tbname from {self.stbname})')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        tdSql.query(f'select count(*) from {self.stbname}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        rows = [2, 0]
        function_names = ['count', 'hyperloglog']
        for i in range(2):
            function_name = function_names[i]
            row = rows[i]
            tdSql.query(f'select {function_name}(tbname) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(ts) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(1) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1),sum(c2),max(1) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, 0)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            tdSql.query(f'select sum(1),{function_name}(1),max(c2) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, None)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, None)
            tdSql.query(f'select {function_name}(1),sum(1),max(c2),min(1),min(2),min(3),min(4),min(5),min(6),min(7),min(8) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(11)
            tdSql.checkData(0, 0, 0)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            tdSql.checkData(0, 10, None)
            tdSql.query(f'select sum(1),max(c2),min(1),leastsquares(c1,1,1) from {self.stbname}')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} group by tbname')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} group by c1')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} group by t0')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by tbname')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by c1')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by t0')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(1) from (select {function_name}(c1),sum(c1) from {self.stbname} group by c1)')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by tbname interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by c1 interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(1),sum(1) from (select {function_name}(1) from {self.stbname} group by tbname order by tbname)')
            tdSql.checkRows(1)
            if 'count' == function_name:
              tdSql.checkData(0, 0, 2)
              tdSql.checkData(0, 1, 2)
            elif 'hyperloglog' == function_name:
              tdSql.checkData(0, 0, 0)
              tdSql.checkData(0, 1, None)

    def query_empty_ntb(self):
        tdSql.query(f'select count(*) from {self.ntbname}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        rows = [1, 0]
        function_names = ['count', 'hyperloglog']
        for i in range(2):
            function_name = function_names[i]
            row = rows[i]
            tdSql.query(f'select {function_name}(tbname) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(ts) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(1) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1),sum(c2),max(1) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, 0)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            tdSql.query(f'select sum(1),{function_name}(1),max(c2) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, None)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, None)
            tdSql.query(f'select {function_name}(1),sum(1),max(c2),min(1),min(2),min(3),min(4),min(5),min(6),min(7),min(8) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(11)
            tdSql.checkData(0, 0, 0)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            tdSql.checkData(0, 10, None)
            tdSql.query(f'select sum(1),max(c2),min(1),leastsquares(c1,1,1) from {self.ntbname}')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} group by tbname')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} group by c1')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(1) from (select {function_name}(c1),sum(c1) from {self.ntbname} group by c1)')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} partition by tbname interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} partition by c1 interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select count(1),sum(1) from (select count(1) from {self.ntbname} group by tbname order by tbname)')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 1)

    def count_query_stb(self,column_dict,tag_dict,stbname,tbnum,rownum):
        tdSql.query(f'select count(tbname) from {stbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        tdSql.query(f'SELECT count(*) from (select distinct tbname from {stbname})')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum)
        for k in column_dict.keys():
            self.query_stb(k,stbname,tbnum,rownum)
        for k in tag_dict.keys():
            self.query_stb(k,stbname,tbnum,rownum)
    def count_query_ctb(self,column_dict,tag_dict,stbname,tbnum,rownum):
        for i in range(tbnum):
            tdSql.query(f'select count(tbname) from {stbname}_{i}')
            tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
            for k in column_dict.keys():
                self.query_ctb(k,i,stbname,rownum)
            for k in tag_dict.keys():
                self.query_ctb(k,i,stbname,rownum)
    def count_query_ntb(self,column_dict,ntbname,rownum):
        tdSql.query(f'select count(tbname) from {ntbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        for k in column_dict.keys():
            self.query_ntb(k,ntbname,rownum)
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def check_ntb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.query_empty_ntb()
        tdSql.execute('flush database db')
        tdSql.query(f'select count(tbname) from {self.ntbname}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        self.count_query_ntb(self.column_dict,self.ntbname,self.rowNum)
        tdSql.execute('flush database db')
        self.count_query_ntb(self.column_dict,self.ntbname,self.rowNum)
        tdSql.execute('drop database db')
    def check_stb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[i]})')
        self.query_empty_stb()
        tdSql.execute('flush database db')
        tdSql.query(f'select count(tbname) from {self.stbname}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query(f'SELECT count(*) from (select distinct tbname from {self.stbname})')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        for i in range(self.tbnum):
            self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
        self.count_query_stb(self.column_dict,self.tag_dict,self.stbname,self.tbnum,self.rowNum)
        self.count_query_ctb(self.column_dict,self.tag_dict,self.stbname,self.tbnum,self.rowNum)
        tdSql.execute('flush database db')
        self.count_query_stb(self.column_dict,self.tag_dict,self.stbname,self.tbnum,self.rowNum)
        self.count_query_ctb(self.column_dict,self.tag_dict,self.stbname,self.tbnum,self.rowNum)
        tdSql.execute('drop database db')
    def run(self):
        self.check_stb()
        self.check_ntb()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
