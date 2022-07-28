from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(),False)
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
        tdSql.query(f'select count(tbname) from {self.ntbname}')
        tdSql.checkRows(0)
        tdSql.execute('flush database db')
        tdSql.query(f'select count(tbname) from {self.ntbname}')
        tdSql.checkRows(0)
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
        tdSql.query(f'SELECT count(*) from (select distinct tbname from {self.stbname})')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        tdSql.query(f'select count(tbname) from {self.stbname}')
        tdSql.checkRows(0)
        tdSql.execute('flush database db')
        tdSql.query(f'select count(tbname) from {self.stbname}')
        tdSql.checkRows(0)
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
