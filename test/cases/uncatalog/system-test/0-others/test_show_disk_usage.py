from new_test_framework.utils import tdLog, tdSql, tdCom
from itertools import product
import taos
import random
import time
from taos.tmq import *
import os
import subprocess

def get_disk_usage(path):
    try:
        result = subprocess.run(['du', '-sb', path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            # The output is in the format "size\tpath"
            size = int(result.stdout.split()[0])
            return size
        else:
            print(f"Error: {result.stderr}")
            return None
    except Exception as e:
        print(f"Exception occurred: {e}")
        return None

def list_directories_with_keyword(base_path, keyword):
    matching_dirs = []
    for dirpath, dirnames, filenames in os.walk(base_path):
        for dirname in dirnames:
            if keyword in dirname:
                full_path = os.path.join(dirpath, dirname)
                matching_dirs.append(full_path)
    return matching_dirs

def calculate_directories_size(base_path, keyword):
    matching_dirs = list_directories_with_keyword(base_path, keyword)
    total_size = 0
    for directory in matching_dirs:
        tdLog.info("directory: %s" % directory)
        size = get_disk_usage(directory)
        if size is not None:
            total_size += size
    return int(total_size/1024)
class TestShowDiskUsage:
    """This test case is used to veirfy show db disk usage"""

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.dbname = "db_disk_usage"   
        cls.stname = "st"
        cls.ctnum = 100 
        cls.row_num = 1000 
        cls.row_data_size = cls.ctnum * cls.row_num * (8 + 4 + 4) # timestamp + int + float
        cls.other_dbname = "db_disk_usage_other"
        cls.other_stname = "st_other"
        cls.data_path = tdCom.getTaosdPath()
        tdLog.debug("data_path: %s" % cls.data_path)
        # create database
        tdSql.execute(f'create database if not exists {cls.dbname};')
        tdSql.execute(f'create database if not exists {cls.other_dbname};')
        tdSql.execute(f'create table {cls.other_dbname}.{cls.other_stname} (ts timestamp, id int, temperature float) tags (name binary(20));')
        tdSql.execute(f'create database if not exists {cls.other_dbname};')

        tdSql.execute(f'use {cls.dbname};')
        # create super table
        tdSql.execute(f'create table {cls.dbname}.{cls.stname} (ts timestamp, id int, temperature float) tags (name binary(20));')
        # create child table
        for i in range(cls.ctnum):
            tdSql.execute(f'create table ct_{str(i+1)} using {cls.stname} tags ("name{str(i+1)}");')
            # insert data
            sql = f"insert into ct_{str(i+1)} values "
            for j in range(cls.row_num):
                sql += f"(now+{j+1}s, {j+1}, {random.uniform(15, 30)}) "
            sql += ";"
            tdSql.execute(sql)
        
        
        tdSql.execute(f"flush database {cls.dbname};")
        tdLog.debug("init finished")
    
    def getWALSize(self): 
        return calculate_directories_size(self.data_path, "wal")
    def getTSDBSize(self): 
        tsdbDirSize = calculate_directories_size(self.data_path, "tsdb")
        cacheRdbSize = calculate_directories_size(self.data_path, "cache.rdb")   
        return tsdbDirSize - cacheRdbSize
    def getTableMetaSize(self):
        return calculate_directories_size(self.data_path, "meta")
    def getCacheRDBSize(self):
        return calculate_directories_size(self.data_path, "cache.rdb")
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
                    tValue = value[0:len(value) - 1]
                    compress_radio = float(tValue)
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
            for j in range(self.row_num):
                sql += f"(now+{j+1}s, {j+1}, {random.uniform(15, 30)}) "
            sql += ";"
            tdSql.execute(sql)
        
        tdSql.execute(f"flush database {self.other_dbname};")
        tdLog.debug("init finished")
    def value_check(self,base_value,check_value, threshold):
        if abs(base_value-check_value) < threshold:
            tdLog.info(f"checkEqual success, base_value={base_value},check_value={check_value}") 
        else :
            tdLog.exit(f"checkEqual error, base_value=={base_value},check_value={check_value}")

    def test_show_disk_usage(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """

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
        tdSql.query(f"select sum(ss) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}';")
        tdSql.checkData(0,0,0)
        tdSql.error(f"select sum(ss) from information_schema.ins_disk_usage  where db='{self.other_dbname}';")
        tdSql.error(f"select sum(ss) from information_schema.ins_disk_usage  where db1='{self.other_dbname}';")


        self.insertData()
        tdSql.execute(f"flush database {self.other_dbname};")
        tdSql.query(f"show {self.other_dbname}.disk_info;")
        disk_occupied,compress_radio = self.checkRes(tdSql.queryResult)
        tdLog.debug("database: %s, disk_occupied: %s, compress_radio: %s" % (self.other_dbname,disk_occupied, compress_radio))
          
        tdSql.query(f"select sum(data1+data2+data3) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}';")
        tdSql.checkData(0,0,disk_occupied) 


        tdSql.query(f"select sum(wal) from information_schema.ins_disk_usage where db_name='{self.other_dbname}' or db_name='{self.dbname}';")
        tdSql.checkRows(1) 
        iwal = tdSql.queryResult[0][0]  
        tdSql.query(f"select sum(table_meta) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}' or db_name='{self.dbname}';")
        itableMeta = tdSql.queryResult[0][0]  
        tdSql.query(f"select sum(data1+data2+data3) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}' or db_name='{self.dbname}';")
        itsdbSize = int(tdSql.queryResult[0][0])  
        tdSql.query(f"select sum(cache_rdb) from information_schema.ins_disk_usage  where db_name='{self.other_dbname}' or db_name='{self.dbname}';")
        icache = tdSql.queryResult[0][0]  
        walSize = self.getWALSize()  
        tableMetaSize = self.getTableMetaSize()  
        tsdbSize = self.getTSDBSize()
        cacheRdbSize = self.getCacheRDBSize()
        tdLog.debug("calc: walSize: %s, tableMetaSize: %s, tsdbSize: %s, cacheRdbSize: %s" % (iwal, itableMeta, itsdbSize, icache))
        tdLog.debug("du: walSize: %s, tableMetaSize: %s, tsdbSize: %s, cacheRdbSize: %s" % (walSize, tableMetaSize, tsdbSize, cacheRdbSize))

        self.value_check(icache, cacheRdbSize, 64)
        self.value_check(itableMeta,tableMetaSize, 64)
        self.value_check(itsdbSize, tsdbSize, 64)
        self.value_check(iwal, walSize, 256)
        #if abs(icache - cacheRdbSize) > 12:
        #    tdLog.error("cache_rdb size is not equal")
        
        #if abs(walSize - iwal) > 12:
        #    tdLog.error("wal size is not equal")

        #if abs(tableMetaSize - itableMeta) > 12k'k'k
        #    tdLog.error("table_meta size is not equal")
        
        #if abs(tsdbSize - itsdbSize) > 12:
        #    tdLog.error("tsdb size is not equal")

        # remove the user
        tdSql.execute(f'drop database {self.dbname};')
        # close the connection
        tdLog.success("%s successfully executed" % __file__)

