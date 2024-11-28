from itertools import product
import taos
import random
import time
from taos.tmq import *
from util.cases import *
from util.common import *
from util.log import *
from util.sql import *
from util.sqlset import *


class TDTestCase:
    """This test case is used to veirfy show db disk usage"""

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        # init the tdsql
        tdSql.init(conn.cursor())
        self.dbname = "db_disk_usage"   
        self.stname = "st"
        self.ctnum = 100 
        self.row_num = 1000 
        self.row_data_size = self.ctnum * self.row_num * (8 + 4 + 4) # timestamp + int + float
        self.other_dbname = "db_disk_usage_other"
        self.other_stname = "st_other"

        # create database
        tdSql.execute(f'create database if not exists {self.dbname};')
        tdSql.execute(f'create database if not exists {self.other_dbname};')
        tdSql.execute(f'create table {self.other_dbname}.{self.other_stname} (ts timestamp, id int, temperature float) tags (name binary(20));')
        tdSql.execute(f'create database if not exists {self.other_dbname};')

        tdSql.execute(f'use {self.dbname};')
        # create super table
        tdSql.execute(f'create table {self.dbname}.{self.stname} (ts timestamp, id int, temperature float) tags (name binary(20));')
        # create child table
        for i in range(self.ctnum):
            tdSql.execute(f'create table ct_{str(i+1)} using {self.stname} tags ("name{str(i+1)}");')
            # insert data
            sql = f"insert into ct_{str(i+1)} values "
            for j in range(self.row_num):
                sql += f"(now+{j+1}s, {j+1}, {random.uniform(15, 30)}) "
            sql += ";"
            tdSql.execute(sql)
        
        
        tdSql.execute(f"flush database {self.dbname};")
        tdLog.debug("init finished")
    
    def checkRes(self, queryRes):
        disk_occupied = 0
        compress_radio = 0
        for item in queryRes:
            if "Disk_occupied=" in item[0]:
                disk_occupied= int(item[0].split("=")[1].split(" ")[0].replace("[", "").replace("k]", ""))
                #tdLog.debug("disk_occupied: %s" % disk_occupied)
            elif "Compress_radio=" in item[0]:
                value = item[0].split("=")[1].split(" ")[0].replace("[", "").replace("]", "")
                if value != 'NULL': 
                    compress_radio = float(value)
                #tdLog.debug("compress_occupied: %s" % compress_radio)
        return disk_occupied, compress_radio 

    def insertData(self): 
        tdSql.execute(f'use {self.other_dbname};')
        # create super table
        tdSql.execute(f'create table {self.other_dbname}.{self.stname} (ts timestamp, id int, temperature float) tags (name binary(20));')
        # create child table
        for i in range(self.ctnum):
            tdSql.execute(f'create table ct_{str(i+1)} using {self.stname} tags ("name{str(i+1)}");')
            sql = f"insert into ct_{str(i+1)} values "
            for j in range(self.row_num * 2):
                sql += f"(now+{j+1}s, {j+1}, {random.uniform(15, 30)}) "
            sql += ";"
            tdSql.execute(sql)
        
        tdSql.execute(f"flush database {self.other_dbname};")
        tdLog.debug("init finished")

    def run(self):

        tdSql.execute(f"flush database {self.dbname};")
        tdSql.query(f"show disk_info")
        tdLog.debug(tdSql.queryResult)
        disk_occupied,compress_radio = self.checkRes(tdSql.queryResult) 
        tdLog.debug("disk_occupied: %s, compress_radio: %s" % (disk_occupied, compress_radio))
        

        #mem_rows_num, stt_rows_num = self.checkRes(tdSql.queryResult)
        #tdLog.debug("mem_rows_num: %s, stt_rows_num: %s" % (mem_rows_num, stt_rows_num))

        tdSql.query(f"select sum(data1+data2+data3) from information_schema.ins_disk_usage  where db_name='{self.dbname}';")
        tdSql.checkData(0,0,disk_occupied) 
        tdSql.query(f"select sum(data1+data2+data3)/sum(raw_data) from information_schema.ins_disk_usage  where db_name='{self.dbname}';")
        #tdSql.checkData(0,0,compress_radio/100) 
        tdSql.query(f"select sum(wal) from information_schema.ins_disk_usage  where db_name='{self.dbname}';")
        tdSql.query(f"select sum(table_meta) from information_schema.ins_disk_usage  where db_name='{self.dbname}';")
        tdSql.query(f"select sum(cache_rdb) from information_schema.ins_disk_usage  where db_name='{self.dbname}';")

        tdSql.execute(f"use {self.other_dbname};") 
        tdSql.query(f"select sum(data1+data2+data3) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}';")
        tdSql.checkData(0,0,0)
        tdSql.query(f"select sum(wal) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}';")
        tdSql.checkData(0,0,0)
        tdSql.query(f"select sum(cache_rdb) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}';")
        tdSql.checkData(0,0,12)
        tdSql.query(f"select sum(table_meta) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}';")
        tdSql.checkData(0,0,152)
        tdSql.query(f"select sum(s3) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}';")
        tdSql.checkData(0,0,0)
        tdSql.error(f"select sum(s3) from information_schema.ins_disk_usage  where db='{self.other_dbname}';")
        tdSql.error(f"select sum(s3) from information_schema.ins_disk_usage  where db1='{self.other_dbname}';")


        self.insertData()
        tdSql.execute(f"flush database {self.other_dbname};")
        tdSql.query(f"show {self.other_dbname}.disk_info;")
        disk_occupied,compress_radio = self.checkRes(tdSql.queryResult)
        tdLog.debug("database: %s, disk_occupied: %s, compress_radio: %s" % (self.other_dbname,disk_occupied, compress_radio))
          
        tdSql.query(f"select sum(data1+data2+data3) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}';")
        tdSql.checkData(0,0,disk_occupied) 
         

    def stop(self):
        # remove the user
        tdSql.execute(f'drop database {self.dbname};')
        # close the connection
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
