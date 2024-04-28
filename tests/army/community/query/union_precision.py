from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *

# different  precision db, execute union query, check time display
# 2024.04.17 just support query same precision tables and db

class TDTestCase(TBase):
    """
    different precision database, execute union and union all query,check time display
    """

    def init(self, conn, logSql,replicaVar=1):
        self.replicaVar= int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = ['ts_ns','ts_ms','ts_us']
        self.stable = 'meters'

        self.table = 'd0'
        self.inser_value = [('2017-07-14T10:40:00.000+08:00',10.2,219,0.31),
                            ('2017-07-14T10:40:10.000+08:00',10.2,219,0.31)
                            ]
        self.tag_value1 = 7 # groupid
        self.tag_value2 = "'bj'" #location


    def prepareData(self,dbname):
        # db
        tdSql.execute(f"create database {dbname};")
        tdSql.execute(f"use {dbname};")
        tdLog.debug(f"Create database {dbname}")

        # super table
        tdSql.execute(
            f"create stable {self.stable} (ts timestamp, current float, voltage int, phase float) tags (groupid int,location varchar(24));")
        tdLog.debug("Create super table %s" % self.stable)

        # subtable
        tdSql.execute(
            f"create table {self.table} using {self.stable}(groupid, location)tags({self.tag_value1},{self.tag_value2});")
        tdLog.debug("Create common table %s" % self.table)

        # insert data

        for d in self.inser_value:
            sql = "insert into {} values".format(self.table)+"{}".format(d) + ";"
            tdSql.execute(sql)
            tdLog.debug("Insert data into table %s" % self.table)


    def test_union_precision_query(self, dbname):
        # 3 kinds of precision ms,ns,us
        tdSql.execute(f"use {dbname};")
        sql = f'''select *
                  from {self.stable} union select * from {self.stable} order by ts desc;'''
        tdSql.query(sql)
        tdSql.checkPartdata(0, 0, "2017-07-14",10)
        tdSql.checkPartdata(1, 0, "2017-07-14",10)

        sql2 = f'''select *
                  from {self.stable} union all select * from {self.stable}  order by ts desc;;'''
        tdSql.query(sql2)
        tdSql.checkPartdata(0, 0, "2017-07-14", 10)
        tdSql.checkPartdata(1, 0, "2017-07-14", 10)
        tdSql.checkPartdata(2, 0, "2017-07-14", 10)
        tdSql.checkPartdata(3, 0, "2017-07-14", 10)


    def run(self):
        for i in self.dbname:
            self.prepareData(i)
            self.test_interval_query(i)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())