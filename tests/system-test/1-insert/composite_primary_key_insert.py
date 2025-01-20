from enum import Enum

from util.log import *
from util.sql import *
from util.cases import *
from util.csv import *
import platform
import os
import taos
import json
from taos import SmlProtocol, SmlPrecision
from taos.error import SchemalessError


class IllegalData(Enum):
    NULL       = 'null'
    # EMPTY      = '\'\''
    NONE       = 'none'
    # TRUE       = 'true'
    # FALSE      = 'false'

# class IllegalDataVar(Enum):
#     NULL       = 'null'
#     EMPTY      = '\'\''
#     NONE       = 'none'
#     TRUE       = 'true'
#     FALSE      = 'false'


class LegalDataType(Enum):  
    INT        = 'INT'
    UINT       = 'INT UNSIGNED'
    BIGINT     = 'BIGINT' 
    UBIGINT    = 'BIGINT UNSIGNED'  
    VARCHAR    = 'VARCHAR(10000)' 
    BINARY     = 'BINARY(10000)'   


class TableType(Enum):  
    SUPERTABLE     = 0 
    CHILDTABLE     = 1 
    NORNALTABLE    = 2 

class HasPK(Enum):  
    NO     = 0 
    YES     = 1 

SHOW_LOG = True
STAET_TS = '2023-10-01 00:00:00.000'

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        self.database = "db_insert_composite_primary_key"
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        # tdSql.execute(f"insert into db4096.ctb00 file '{self.testcasePath}//tableColumn4096csvLength64k.csv'")

        self.tdCsv = TDCsv()
        self.tdCsv.file_path = self.testcasePath
        self.tdCsv.file_name = 'file1.csv'

        self.stable_name = 's_table'
        self.ctable_name = 'c_table'
        self.ntable_name = 'n_table'


    def prepare_db(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database}")
        tdSql.execute(f"use {self.database}")

    def get_latest_ts(self, table_name: str):
        tdSql.query(f'select last(ts) from {table_name}')
        now_time = tdSql.queryResult[0][0].strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"'{now_time}'"
    
    def test_insert_data(self, dtype: LegalDataType, hasPk: HasPK):
        # drop super table and child table
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"drop table if exists {self.ntable_name}_1")
        tdSql.execute(f"drop table if exists {self.ntable_name}_2")
        if hasPk:
            tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}, c3 {dtype.value}) tags (engine int)", show=SHOW_LOG)
        else:
            tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value}, c2 {dtype.value}, c3 {dtype.value}) tags (engine int)", show=SHOW_LOG)

        # 1.insert into value through supper table
        table_name = f'{self.ctable_name}_1'
        tdSql.execute(f"insert into {self.stable_name} (tbname, engine, ts, pk, c2) values('{table_name}', 1, now, 1, '1')", show=SHOW_LOG)
        current_ts1 = self.get_latest_ts(self.stable_name)
        tdSql.execute(f"insert into {self.stable_name} (tbname, engine, ts, pk, c2) values('{table_name}', 1, {current_ts1}, 2, '2')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.stable_name} (tbname, engine, ts, pk, c2) values('{table_name}', 1, {current_ts1}, 3, '3')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.stable_name} (tbname, engine, ts, pk, c2) values('{table_name}', 1, now + 1s, 1, '4')", show=SHOW_LOG)
        current_ts2 = self.get_latest_ts(self.stable_name)
        tdSql.execute(f"insert into {self.stable_name} (tbname, engine, ts, pk, c2) values('{table_name}', 1, {current_ts2}, 2, '5')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.stable_name} (tbname, engine, ts, pk) values('{table_name}', 1, now + 2s, 2)", show=SHOW_LOG)

        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(6)
        tdSql.query(f"select * from {self.stable_name} where engine = 1 and ts ={current_ts1}", show=SHOW_LOG)
        tdSql.checkRows(3)
        tdSql.query(f"select * from {self.stable_name} where engine = 1 and ts ={current_ts2}", show=SHOW_LOG)
        tdSql.checkRows(2)
        tdSql.query(f"select * from {self.stable_name} where engine = 1 and ts ={current_ts2} and pk = 2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(self.stable_name)

        # 2.insert into value through child table
        table_name = f'{self.ctable_name}_2'
        tdSql.execute(f"insert into {table_name} using {self.stable_name} tags(2) values(now, 1, '7', '6')", show=SHOW_LOG)
        current_ts3 = self.get_latest_ts(table_name)
        tdSql.execute(f"insert into {table_name} values({current_ts3}, 2, '8', '6') ({current_ts3}, 3, '9', '6')", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name} using {self.stable_name} tags(2) values(now + 2s, 1, '10', '6')", show=SHOW_LOG)
        current_ts4 = self.get_latest_ts(table_name)
        tdSql.execute(f"insert into {table_name} using {self.stable_name} tags(2) (ts, pk, c2) values({current_ts4}, 2, '11')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name} using {self.stable_name} tags(2) (ts, pk) values(now + 4s, 2)", show=SHOW_LOG)

        tdSql.query(f'select * from {table_name}')
        tdSql.checkRows(6)
        tdSql.query(f"select * from {table_name} where engine = 2 and ts ={current_ts3}", show=SHOW_LOG)
        tdSql.checkRows(3)
        tdSql.query(f"select * from {table_name} where engine = 2 and ts ={current_ts4}", show=SHOW_LOG)
        tdSql.checkRows(2)
        tdSql.query(f"select * from {table_name} where engine = 2 and ts ={current_ts4} and pk = 2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(table_name)

        # 3.insert value into child table from csv file
        data = [
            ['ts','pk','c2'],
            ['\'2024-03-29 16:55:42.572\'','1','1','100'],
            ['\'2024-03-29 16:55:42.572\'','2','2','100'],
            ['\'2024-03-29 16:55:42.572\'','3','3','100'],
            ['\'2024-03-29 16:55:43.586\'','1','4','100'],
            ['\'2024-03-29 16:55:43.586\'','2','5','100'],
            ['\'2024-03-29 16:55:44.595\'','2','6','100']
        ]

        self.tdCsv.delete()
        self.tdCsv.write(data)

        table_name = f'{self.ctable_name}_3'
        tdSql.execute(f"create table {table_name} using {self.stable_name} tags(3)", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name} file '{self.tdCsv.file}'", show=SHOW_LOG)

        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from {table_name}')
        tdSql.checkRows(6)
        tdSql.query(f"select * from {table_name} where engine=3 and ts='2024-03-29 16:55:42.572'", show=SHOW_LOG)
        tdSql.checkRows(3)
        tdSql.query(f"select * from {table_name} where engine=3 and ts='2024-03-29 16:55:43.586'", show=SHOW_LOG)
        tdSql.checkRows(2)
        tdSql.query(f"select * from {table_name} where engine=3 and ts='2024-03-29 16:55:43.586' and pk=2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(table_name)

        # 4.insert value into child table from csv file, create table automatically
        data = [
            ['ts','pk','c2'],
            ['\'2024-03-28 16:55:42.572\'','1','1','100'],
            ['\'2024-03-28 16:55:42.572\'','2','2','100'],
            ['\'2024-03-28 16:55:42.572\'','3','3','100'],
            ['\'2024-03-28 16:55:43.586\'','1','4','100'],
            ['\'2024-03-28 16:55:43.586\'','2','5','100'],
            ['\'2024-03-28 16:55:44.595\'','2','6','100']
        ]

        self.tdCsv.delete()
        self.tdCsv.write(data)

        table_name = f'{self.ctable_name}_4'
        self._check_select(self.stable_name)
        tdSql.execute(f"insert into {table_name} using {self.stable_name} tags(4) file '{self.tdCsv.file}'", show=SHOW_LOG)

        tdSql.query(f'select * from {table_name}')
        tdSql.checkRows(6)
        tdSql.query(f"select * from {self.stable_name} where engine=4 and ts='2024-03-28 16:55:42.572'", show=SHOW_LOG)
        tdSql.checkRows(3)
        tdSql.query(f"select * from {self.stable_name} where engine=4 and ts='2024-03-28 16:55:43.586'", show=SHOW_LOG)
        tdSql.checkRows(2)
        tdSql.query(f"select * from {self.stable_name} where engine=4 and ts='2024-03-28 16:55:43.586' and pk=2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(self.stable_name)

        # 5.insert value into normal table from csv file
        table_name = f'{self.ntable_name}_1'
        if hasPk:
            tdSql.execute(f"create table {table_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}, c3 {dtype.value})", show=SHOW_LOG)
        else:
            tdSql.execute(f"create table {table_name} (ts timestamp, pk {dtype.value}, c2 {dtype.value}, c3 {dtype.value})", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name} file '{self.tdCsv.file}'", show=SHOW_LOG)

        tdSql.query(f'select * from {table_name}')
        tdSql.checkRows(6)
        tdSql.query(f"select * from {table_name} where ts='2024-03-28 16:55:42.572'", show=SHOW_LOG)
        tdSql.checkRows(3)
        tdSql.query(f"select * from {table_name} where ts='2024-03-28 16:55:43.586'", show=SHOW_LOG)
        tdSql.checkRows(2)
        tdSql.query(f"select * from {table_name} where ts='2024-03-28 16:55:43.586' and pk=2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(table_name)

        # 6.insert value into normal table
        table_name = f'{self.ntable_name}_2'
        if hasPk:
            tdSql.execute(f"create table {table_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}, c3 {dtype.value})", show=SHOW_LOG)
        else:
            tdSql.execute(f"create table {table_name} (ts timestamp, pk {dtype.value}, c2 {dtype.value}, c3 {dtype.value})", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name} values(now, 1, '1', '234')", show=SHOW_LOG)
        current_ts1 = self.get_latest_ts(table_name)
        tdSql.execute(f"insert into {table_name} values({current_ts1}, 2, '2', '234') ({current_ts1}, 3, '3', '234')", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name} values(now + 1s, 1, '4', '234')", show=SHOW_LOG)
        current_ts2 = self.get_latest_ts(table_name)
        tdSql.execute(f"insert into {table_name} (ts, pk, c2) values({current_ts2}, 2, '5')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name} (ts, pk) values(now + 2s, 2)", show=SHOW_LOG)

        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from {table_name}')
        tdSql.checkRows(6)
        tdSql.query(f"select * from {table_name} where ts ={current_ts1}", show=SHOW_LOG)
        tdSql.checkRows(3)
        tdSql.query(f"select * from {table_name} where ts ={current_ts2}", show=SHOW_LOG)
        tdSql.checkRows(2)
        tdSql.query(f"select * from {table_name} where ts ={current_ts2} and pk = 2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(table_name)

    def test_insert_data_illegal(self, dtype: LegalDataType, illegal_data: IllegalData):
        # drop tables
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"drop table if exists {self.ntable_name}_1")
        tdSql.execute(f"drop table if exists {self.ntable_name}_2")
        
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}) tags (engine int)", show=SHOW_LOG)

        # 1.insert into value through supper table
        table_name = f'{self.ctable_name}_1'
        tdSql.error(f"insert into {self.stable_name} (tbname, engine, ts, pk, c2) values('{table_name}', 1, now, {illegal_data.value}, '1')", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        # 2.insert into value through child table
        table_name = f'{self.ctable_name}_2'
        tdSql.error(f"insert into {table_name} using {self.stable_name} tags(2) values(now, {illegal_data.value}, '7')", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        # 4.insert value into child table from csv file
        data = [
            ['ts','pk','c2'],
            ['\'2024-03-29 16:55:42.572\'','1','1'],
            ['\'2024-03-29 16:55:42.572\'',f'{illegal_data.value}','2'],
            ['\'2024-03-29 16:55:42.572\'','3','3'],
            ['\'2024-03-29 16:55:43.586\'','1','4'],
            ['\'2024-03-29 16:55:43.586\'','2','5'],
            ['\'2024-03-29 16:55:44.595\'','2','6']
        ]

        self.tdCsv.delete()
        self.tdCsv.write(data)

        table_name = f'{self.ctable_name}_3'
        tdSql.execute(f"create table {table_name} using {self.stable_name} tags(3)", show=SHOW_LOG)
        tdSql.error(f"insert into {table_name} file '{self.tdCsv.file}'", show=SHOW_LOG)
        tdSql.query(f'select * from {table_name}')
        tdSql.checkRows(0)
        self._check_select(table_name)

        # 5.insert value into child table from csv file, create table automatically
        table_name = f'{self.ctable_name}_4'
        tdSql.error(f"insert into {table_name} using {self.stable_name} tags(4) file '{self.tdCsv.file}'", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        # 6.insert value into normal table from csv file
        table_name = f'{self.ntable_name}_1'
        tdSql.execute(f"create table {table_name} using {self.stable_name} tags(3)", show=SHOW_LOG)
        tdSql.error(f"insert into {table_name} file '{self.tdCsv.file}'", show=SHOW_LOG)
        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from {table_name}')
        tdSql.checkRows(0)
        self._check_select(table_name)

        # 7.insert value into normal table
        table_name = f'{self.ntable_name}_2'
        tdSql.execute(f"create table {table_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value})", show=SHOW_LOG)
        tdSql.error(f"insert into {table_name} values(now, {illegal_data.value}, '1')", show=SHOW_LOG)
        tdSql.query(f'select * from {table_name}')
        tdSql.checkRows(0)
        self._check_select(table_name)
    
    def test_insert_select(self, dtype: LegalDataType):
        # # 1.pk table to non-pk table, throw error
        tdSql.execute(f"drop table if exists source_{self.stable_name}")
        tdSql.execute(f"drop table if exists source_{self.ctable_name}")
        tdSql.execute(f"drop table if exists source_{self.ntable_name}")
        tdSql.execute(f"drop table if exists dest_{self.stable_name}")
        tdSql.execute(f"drop table if exists dest_{self.ntable_name}")

        tdSql.execute(f"create table source_{self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}) tags (engine int)", show=SHOW_LOG)
        tdSql.execute(f"create table source_{self.ctable_name} using source_{self.stable_name} tags(3)", show=SHOW_LOG)
        tdSql.execute(f"create table source_{self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value})", show=SHOW_LOG)
        tdSql.execute(f"create table dest_{self.ntable_name} (ts timestamp, pk {dtype.value}, c2 {dtype.value})", show=SHOW_LOG)

        tdSql.error(f"insert into dest_{self.ntable_name} select * from source_{self.stable_name})", show=SHOW_LOG)
        tdSql.error(f"insert into dest_{self.ntable_name} select * from source_{self.ctable_name})", show=SHOW_LOG)
        tdSql.error(f"insert into dest_{self.ntable_name} select * from source_{self.ntable_name})", show=SHOW_LOG)

        # 2.non-pk table to pk table
        tdSql.execute(f"drop table if exists source_{self.stable_name}")
        tdSql.execute(f"drop table if exists source_{self.ctable_name}")
        tdSql.execute(f"drop table if exists source_{self.ntable_name}")
        tdSql.execute(f"drop table if exists dest_{self.stable_name}")
        tdSql.execute(f"drop table if exists dest_{self.ntable_name}")

        # create dest super & child table
        tdSql.execute(f"create table dest_{self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}) tags (engine int)", show=SHOW_LOG)
        tdSql.execute(f"create table dest_{self.ctable_name} using dest_{self.stable_name} tags(3)", show=SHOW_LOG)
        # tdSql.execute(f"insert into source_{self.ctable_name} values(now, 1, 1) (now+1s, 2, 2) (now+2s, 3, 3)", show=SHOW_LOG)

        # create normal dest table
        tdSql.execute(f"create table dest_{self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value})", show=SHOW_LOG)

        # create source table & insert data
        tdSql.execute(f"create table source_{self.ntable_name} (ts timestamp, pk {dtype.value}, c2 {dtype.value})", show=SHOW_LOG)
        tdSql.execute(f"insert into source_{self.ntable_name} values(now, 1, 1) (now+1s, 2, 2) (now+2s, 3, 3)", show=SHOW_LOG)
        tdSql.query(f'select * from source_{self.ntable_name}')
        source_data = tdSql.queryResult
        
        tdSql.execute(f"insert into dest_{self.ctable_name} select ts, pk, c2 from source_{self.ntable_name}", show=SHOW_LOG)
        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from dest_{self.ctable_name}')
        dest_data = tdSql.queryResult
        self._compare_table_data(source_data, dest_data, 3, 3)
        self._check_select(f'dest_{self.ctable_name}')
        tdSql.execute(f"delete from dest_{self.ctable_name}", show=SHOW_LOG)
        self._check_select(f'dest_{self.ctable_name}')

        tdSql.execute(f"insert into dest_{self.ntable_name} select * from source_{self.ntable_name}", show=SHOW_LOG)
        tdSql.query(f'select * from dest_{self.ntable_name}')
        dest_data = tdSql.queryResult
        self._compare_table_data(source_data, dest_data, 3, 3)
        self._check_select(f'dest_{self.ntable_name}')
        tdSql.execute(f"delete from dest_{self.ntable_name}", show=SHOW_LOG)
        self._check_select(f'dest_{self.ntable_name}')

        # TD-29363
        tdSql.execute(f"drop table if exists source_null")

        tdSql.execute(f"create table source_null (ts timestamp, pk {dtype.value}, c2 {dtype.value})", show=SHOW_LOG)
        tdSql.execute(f"insert into source_null values(now, null, 1) (now+1s, 2, 2) (now+2s, 3, 3)", show=SHOW_LOG)
        tdSql.error(f"insert into dest_{self.ntable_name} values(now, null, 1) (now+1s, 2, 2) (now+2s, 3, 3)", show=SHOW_LOG)
        tdSql.error(f"insert into dest_{self.ctable_name} select ts, pk, c2 from source_null", show=SHOW_LOG)
        tdSql.error(f"insert into dest_{self.ntable_name} select * from source_null", show=SHOW_LOG)

        # 3.pk table to pk table
        tdSql.execute(f"drop table if exists source_{self.stable_name}")
        tdSql.execute(f"drop table if exists source_{self.ctable_name}")
        tdSql.execute(f"drop table if exists source_{self.ntable_name}")
        tdSql.execute(f"drop table if exists dest_{self.stable_name}")
        tdSql.execute(f"drop table if exists dest_{self.ntable_name}")

        # create dest super & child table
        tdSql.execute(f"create table dest_{self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}, c3 {dtype.value}) tags (engine int)", show=SHOW_LOG)
        tdSql.execute(f"create table dest_{self.ctable_name} using dest_{self.stable_name} tags(3)", show=SHOW_LOG)
        tdSql.execute(f"insert into dest_{self.ctable_name} values('2024-04-01 17:38:08.764', 1, 1, 100) ('2024-04-01 17:38:08.764', 2, 2, 200) ('2024-04-01 17:38:08.764', 3, 3, 300)", show=SHOW_LOG)
        tdSql.execute(f"insert into dest_{self.ctable_name} values('2024-04-02 17:38:08.764', 1, 4, 400) ('2024-04-02 17:38:08.764', 2, 1, 500) ('2024-04-02 17:38:08.764', 3, 1, 600)", show=SHOW_LOG)

        # create normal dest table
        tdSql.execute(f"create table dest_{self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}, c3 {dtype.value})", show=SHOW_LOG)
        tdSql.execute(f"insert into dest_{self.ntable_name} values('2024-04-01 17:38:08.764', 1, 1, 100) ('2024-04-01 17:38:08.764', 2, 2, 200) ('2024-04-01 17:38:08.764', 3, 3, 300)", show=SHOW_LOG)
        tdSql.execute(f"insert into dest_{self.ntable_name} values('2024-04-02 17:38:08.764', 1, 4, 400) ('2024-04-02 17:38:08.764', 2, 1, 500) ('2024-04-02 17:38:08.764', 3, 1, 600)", show=SHOW_LOG)

        # create source table & insert data
        tdSql.execute(f"create table source_{self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}, c3 {dtype.value})", show=SHOW_LOG)
        tdSql.execute(f"insert into source_{self.ntable_name} values('2024-04-02 17:38:08.764', 1, 4, 800) ('2024-04-02 17:38:08.764', 2, 5, 400) ('2024-04-02 17:38:08.764', 3, 6, 600)", show=SHOW_LOG)
        tdSql.execute(f"insert into source_{self.ntable_name} values('2024-04-04 17:38:08.764', 1, 7, 365)", show=SHOW_LOG)

        # insert into select 
        tdSql.execute(f"insert into dest_{self.ctable_name} (ts, pk, c2) select ts, pk, c2 from source_{self.ntable_name}", show=SHOW_LOG)
        tdSql.query(f'select * from dest_{self.ctable_name}')
        tdSql.checkRows(7)
        if dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            tdSql.query(f"select * from dest_{self.ctable_name} where c2='5' or c2='6'", show=SHOW_LOG)
        else:
            tdSql.query(f'select * from dest_{self.ctable_name} where c2=5 or c2=6', show=SHOW_LOG)
        tdSql.checkRows(2)
        self._check_select(f'dest_{self.ctable_name}')
        tdSql.execute(f"delete from dest_{self.ctable_name}", show=SHOW_LOG)
        self._check_select(f'dest_{self.ctable_name}')

        tdSql.execute(f"insert into dest_{self.ntable_name} (ts, pk, c2) select ts, pk, c2 from source_{self.ntable_name}", show=SHOW_LOG)
        tdSql.query(f'select * from dest_{self.ntable_name}')
        tdSql.checkRows(7)
        if dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            tdSql.query(f"select * from dest_{self.ntable_name} where c2='5' or c2='6'")
        else:
            tdSql.query(f'select * from dest_{self.ntable_name} where c2=5 or c2=6')
        tdSql.checkRows(2)
        self._check_select(f'dest_{self.ntable_name}')
        tdSql.execute(f"delete from dest_{self.ntable_name}", show=SHOW_LOG)
        self._check_select(f'dest_{self.ntable_name}')

    def test_schemaless_error(self):
        # 5.1.insert into values via influxDB
        lines = ["meters,location=California.LosAngeles,groupid=2 current=11i32,voltage=221,phase=0.28 1648432611249000",
         "meters,location=California.LosAngeles,groupid=2 current=13i32,voltage=223,phase=0.29 1648432611249000",
         "meters,location=California.LosAngeles,groupid=3 current=10i32,voltage=223,phase=0.29 1648432611249300",
         "meters,location=California.LosAngeles,groupid=3 current=11i32,voltage=221,phase=0.35 1648432611249300",
         ]
        try:
            conn = taos.connect()
            conn.execute("drop database if exists influxDB")
            conn.execute("CREATE DATABASE influxDB precision 'us'")
            conn.execute("USE influxDB")
            conn.execute("CREATE STABLE `meters` (`_ts` TIMESTAMP, `current` int primary key, `voltage` DOUBLE, `phase` DOUBLE) TAGS (`location` NCHAR(32), `groupid` NCHAR(2))")
            conn.execute("create table t_be97833a0e1f523fcdaeb6291d6fdf27 using meters tags('California.LosAngeles', 2)")
            conn.execute("create table t_10b65f71ff8970369c8c18de0d6be028 using meters tags('California.LosAngeles', 3)")
            conn.schemaless_insert(lines, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
            tdSql.checkEqual(False, True)
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg == 'Can not insert data into table with primary key', True)

        # 5.2.insert into values via OpenTSDB
        lines = ["meters.current 1648432611249 10i32 location=California.SanFrancisco groupid=2",
         "meters.current 1648432611250 12i32 location=California.SanFrancisco groupid=2",
         "meters.current 1648432611249 10i32 location=California.LosAngeles groupid=3",
         "meters.current 1648432611250 11i32 location=California.LosAngeles groupid=3",
         "meters.voltage 1648432611249 219i32 location=California.SanFrancisco groupid=2",
         "meters.voltage 1648432611250 218i32 location=California.SanFrancisco groupid=2",
         "meters.voltage 1648432611249 221i32 location=California.LosAngeles groupid=3",
         "meters.voltage 1648432611250 217i32 location=California.LosAngeles groupid=3",
         ]
        try:
            conn = taos.connect()
            conn.execute("drop database if exists OpenTSDB")
            conn.execute("CREATE DATABASE OpenTSDB precision 'us'")
            conn.execute("USE OpenTSDB")
            conn.execute("CREATE STABLE `meters_current` (`_ts` TIMESTAMP, `_value` INT primary key) TAGS (`location` NCHAR(32), `groupid` NCHAR(2))")
            conn.execute("CREATE TABLE `t_c66ea0b2497be26ca9d328b59c39dd61` USING `meters_current` (`location`, `groupid`) TAGS ('California.LosAngeles', '3')")
            conn.execute("CREATE TABLE `t_e71c6cf63cfcabb0e261886adea02274` USING `meters_current` (`location`, `groupid`) TAGS ('California.SanFrancisco', '2')")
            conn.schemaless_insert(lines, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            tdSql.checkEqual(False, True)
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg == 'Can not insert data into table with primary key', True)

        # 5.3.insert into values via OpenTSDB Json
        lines = [{"metric": "meters.current", "timestamp": 1648432611249, "value": "a32", "tags": {"location": "California.SanFrancisco", "groupid": 2}}]
        try:
            conn = taos.connect()
            conn.execute("drop database if exists OpenTSDBJson")
            conn.execute("CREATE DATABASE OpenTSDBJson")
            conn.execute("USE OpenTSDBJson")
            conn.execute("CREATE STABLE `meters_current` (`_ts` TIMESTAMP, `_value` varchar(10) primary key) TAGS (`location` VARCHAR(32), `groupid` DOUBLE)")
            conn.execute("CREATE TABLE `t_71d176bfc4c952b64d30d719004807a0` USING `meters_current` (`location`, `groupid`) TAGS ('California.SanFrancisco', 2.000000e+00)")
            # global lines
            lines = json.dumps(lines)
            # note: the first parameter must be a list with only one element.
            conn.schemaless_insert([lines], SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            tdSql.checkEqual(False, True)
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg == 'Can not insert data into table with primary key', True)

        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)

    def test_insert_values_special(self, dtype: LegalDataType):
        # 4.insert into values without ts column 
        # drop tables
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"drop table if exists {self.ntable_name}")
        
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 binary(10)) tags (engine int)", show=SHOW_LOG)

        # # 4.1.insert into value through supper table
        tdSql.error(f"insert into {self.stable_name} (tbname, engine, pk, c2) values('{self.ctable_name}', 1, '1', '1')", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        # # 4.2.insert into value through child table
        tdSql.error(f"insert into {self.ctable_name} using {self.stable_name} tags(2) (pk, c2) values('1', '7')", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        # # 4.3.insert value into normal table
        tdSql.execute(f"create table {self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c2 varchar(20))", show=SHOW_LOG)
        tdSql.error(f"insert into {self.ntable_name} (pk, c2) values('1', '1')", show=SHOW_LOG)
        tdSql.query(f'select * from {self.ntable_name}')
        tdSql.checkRows(0)
        self._check_select(self.ntable_name)

        # 5.insert into values without pk column 
        # drop tables
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"drop table if exists {self.ntable_name}")
        
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 binary(10)) tags (engine int)", show=SHOW_LOG)

        # # 5.1.insert into value through supper table
        tdSql.error(f"insert into {self.stable_name} (tbname, engine, ts, c2) values('{self.ctable_name}', 1, now, '1')", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        # # 5.2.insert into value through child table
        tdSql.error(f"insert into {self.ctable_name} using {self.stable_name} tags(2) (ts, c2) values(now, '7')", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        # # 5.3.insert value into normal table
        tdSql.execute(f"create table {self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c2 varchar(20))", show=SHOW_LOG)
        tdSql.error(f"insert into {self.ntable_name} (ts, c2) values(now, '1')", show=SHOW_LOG)
        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.ntable_name}')
        tdSql.checkRows(0)
        self._check_select(self.ntable_name)

    def test_insert_into_mutiple_tables(self, dtype: LegalDataType):
        # drop super table and child table
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}) tags (engine int)", show=SHOW_LOG)

        # 1.insert into value by supper table syntax
        error_sql = f"insert into {self.stable_name} (tbname, engine, ts, pk, c2) " \
              f"values('{self.ctable_name}_1', 1, '2021-07-13 14:06:34.630', 1, 100) " \
              f"('{self.ctable_name}_1', 1, '2021-07-13 14:06:34.630', 2, 200) " \
              f"('{self.ctable_name}_2', 2, '2021-07-14 14:06:34.630', 1, 300) " \
              f"('{self.ctable_name}_2', 2, '2021-07-14 14:06:34.630', 2, 400) " \
              f"('{self.ctable_name}_3', 3, '2021-07-15 14:06:34.630', null, 500)" 
              
        tdSql.error(error_sql, show=SHOW_LOG)

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)
        
        sql = f"insert into {self.stable_name} (tbname, engine, ts, pk, c2) " \
              f"values('{self.ctable_name}_1', 1, '2021-07-13 14:06:34.630', 1, 100) " \
              f"('{self.ctable_name}_1', 1, '2021-07-13 14:06:34.630', 2, 200) " \
              f"('{self.ctable_name}_2', 2, '2021-07-14 14:06:34.630', 1, 300) " \
              f"('{self.ctable_name}_2', 2, '2021-07-14 14:06:34.630', 2, 400) " \
              f"('{self.ctable_name}_3', 3, '2021-07-15 14:06:34.630', 1, 500)" 
              
        tdSql.execute(sql, show=SHOW_LOG)

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(5)
        tdSql.query(f"select * from {self.stable_name} where ts='2021-07-13 14:06:34.630'", show=SHOW_LOG)
        tdSql.checkRows(2)
        tdSql.query(f"select * from {self.stable_name} where ts='2021-07-13 14:06:34.630' and pk=2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(self.stable_name)

        # 2.insert into value and create table automatically
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}) tags (engine int)", show=SHOW_LOG)

        error_sql = f"insert into {self.ctable_name}_1 using {self.stable_name} tags(1) values('2021-07-13 14:06:34.630', 1, 100) ('2021-07-13 14:06:34.630', 2, 200) " \
                          f"{self.ctable_name}_2 using {self.stable_name} (engine) tags(2) values('2021-07-14 14:06:34.630', 1, 300) ('2021-07-14 14:06:34.630', 2, 400) " \
                          f"{self.ctable_name}_3 using {self.stable_name} (engine) tags(3) values('2021-07-15 14:06:34.630', null, 500)" 
              
        tdSql.error(error_sql, show=SHOW_LOG)
        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        sql = f"insert into {self.ctable_name}_1 using {self.stable_name} tags(1) values('2021-07-13 14:06:34.630', 1, 100) ('2021-07-13 14:06:34.630', 2, 200) " \
                          f"{self.ctable_name}_2 using {self.stable_name} (engine) tags(2) values('2021-07-14 14:06:34.630', 1, 300) ('2021-07-14 14:06:34.630', 2, 400) " \
                          f"{self.ctable_name}_3 using {self.stable_name} (engine) tags(3) values('2021-07-15 14:06:34.630', 1, 500)" 
              
        tdSql.execute(sql, show=SHOW_LOG)

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(5)
        tdSql.query(f"select * from {self.stable_name} where ts='2021-07-13 14:06:34.630'", show=SHOW_LOG)
        tdSql.checkRows(2)
        tdSql.query(f"select * from {self.stable_name} where ts='2021-07-13 14:06:34.630' and pk=2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(self.stable_name)

        # 3.insert value into child table from csv file, create table automatically
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}) tags (engine int)", show=SHOW_LOG)

        error_data = [
            ['ts','pk','c2'],
            ['\'2024-03-29 16:55:42.572\'','1','1'],
            ['\'2024-03-29 16:55:42.572\'','2','2'],
            ['\'2024-03-29 16:55:42.572\'','null','3'],
            ['\'2024-03-29 16:55:43.586\'','1','4'],
            ['\'2024-03-29 16:55:43.586\'','2','5'],
            ['\'2024-03-29 16:55:44.595\'','2','6']
        ]

        self.tdCsv.delete()
        self.tdCsv.write(error_data)
        
        sql = f"insert into {self.ctable_name}_1 using {self.stable_name} tags(1) FILE '{self.tdCsv.file}' " \
                          f"{self.ctable_name}_2 using {self.stable_name} (engine) tags(2) FILE '{self.tdCsv.file}' " \
                          f"{self.ctable_name}_3 using {self.stable_name} (engine) tags(3) FILE '{self.tdCsv.file}'" 
              
        tdSql.error(sql, show=SHOW_LOG)

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        self._check_select(self.stable_name)

        data = [
            ['ts','pk','c2'],
            ['\'2024-03-29 16:55:42.572\'','1','1'],
            ['\'2024-03-29 16:55:42.572\'','2','2'],
            ['\'2024-03-29 16:55:42.572\'','2','3'],
            ['\'2024-03-29 16:55:43.586\'','1','4'],
            ['\'2024-03-29 16:55:43.586\'','2','5'],
            ['\'2024-03-29 16:55:44.595\'','2','6']
        ]

        self.tdCsv.delete()
        self.tdCsv.write(data)

        sql = f"insert into {self.ctable_name}_1 using {self.stable_name} tags(1) FILE '{self.tdCsv.file}'" \
                          f"{self.ctable_name}_2 using {self.stable_name} (engine) tags(2) FILE '{self.tdCsv.file}'" \
                          f"{self.ctable_name}_3 using {self.stable_name} (engine) tags(3) FILE '{self.tdCsv.file}'" 
              
        tdSql.execute(sql, show=SHOW_LOG)

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(15)
        tdSql.query(f"select * from {self.stable_name} where ts='2024-03-29 16:55:42.572'", show=SHOW_LOG)
        tdSql.checkRows(6)
        tdSql.query(f"select * from {self.stable_name} where ts='2024-03-29 16:55:42.572' and pk=2", show=SHOW_LOG)
        tdSql.checkRows(3)
        self._check_select(self.stable_name)

        # 6.insert value into normal table
        tdSql.execute(f"drop table if exists {self.ntable_name}_1")
        tdSql.execute(f"drop table if exists {self.ntable_name}_2")
        tdSql.execute(f"create table {self.ntable_name}_1 (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value})", show=SHOW_LOG)
        tdSql.execute(f"create table {self.ntable_name}_2 (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value})", show=SHOW_LOG)

        sql = f"insert into {self.ntable_name}_1 values('2021-07-13 14:06:34.630', 1, 100) ('2021-07-13 14:06:34.630', 2, 200) " \
                          f"{self.ntable_name}_2 values('2021-07-14 14:06:34.630', 1, 300) ('2021-07-14 14:06:34.630', 2, 400) " 
              
        tdSql.execute(sql, show=SHOW_LOG)

        tdSql.query(f'select * from {self.ntable_name}_1')
        tdSql.checkRows(2)
        tdSql.query(f"select * from {self.ntable_name}_2 where ts='2021-07-14 14:06:34.630' and pk=2", show=SHOW_LOG)
        tdSql.checkRows(1)
        self._check_select(self.ntable_name)

        # 7. insert value into child and normal table
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"drop table if exists {self.ctable_name}")
        tdSql.execute(f"drop table if exists {self.ntable_name}")

        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}) tags (engine int)", show=SHOW_LOG)
        tdSql.execute(f"create table {self.ctable_name} using {self.stable_name} tags (1)", show=SHOW_LOG)
        tdSql.execute(f"create table {self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value})", show=SHOW_LOG)

        error_sql = f"insert into {self.ctable_name} values('2021-07-13 14:06:34.630', null, 100) ('2021-07-13 14:06:34.630', 2, 200) " \
                          f"{self.ntable_name} values('2021-07-14 14:06:34.630', 1, 300) ('2021-07-14 14:06:34.630', 2, 400) " 
        
        sql = f"insert into {self.ctable_name} values('2021-07-13 14:06:34.630', 1, 100) ('2021-07-13 14:06:34.630', 2, 200) " \
                          f"{self.ntable_name} values('2021-07-14 14:06:34.630', 1, 300) ('2021-07-14 14:06:34.630', 2, 400) " 
        
        tdSql.error(error_sql, show=SHOW_LOG)
        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        tdSql.query(f'select * from {self.ntable_name}')
        tdSql.checkRows(0)

        tdSql.execute(sql, show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(2)
        tdSql.query(f'select * from {self.ntable_name}')
        tdSql.checkRows(2)
        self._check_select(self.stable_name)
        self._check_select(self.ntable_name)

    def test_stmt(self, dtype: LegalDataType):
        tdSql.execute(f"drop table if exists {self.stable_name}", show=SHOW_LOG)
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value}, c3 float) tags (engine int)", show=SHOW_LOG)

        sql = f"INSERT INTO ? USING {self.stable_name} TAGS(?) VALUES (?,?,?,?)"

        conn = taos.connect()
        conn.select_db(self.database)
        stmt = conn.statement(sql)
        
        tbname = f"d1001"

        tags = taos.new_bind_params(1)
        tags[0].int([2])

        stmt.set_tbname_tags(tbname, tags)

        params = taos.new_bind_params(4)
        params[0].timestamp((1626861392589, 1626861392589, 1626861392592))
        if dtype == LegalDataType.INT :
            params[1].int((10, 12, 12))
            params[2].int([194, 200, 201])
        elif dtype == LegalDataType.UINT:
            params[1].int_unsigned((10, 12, 12))
            params[2].int_unsigned([194, 200, 201])
        elif dtype == LegalDataType.BIGINT:
            params[1].bigint((10, 12, 12))
            params[2].bigint([194, 200, 201])
        elif dtype == LegalDataType.UBIGINT:
            params[1].bigint_unsigned((10, 12, 12))
            params[2].bigint_unsigned([194, 200, 201])
        elif dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            params[1].binary(("s10", "s12", "s12"))
            params[2].binary(["s194", "s200", "s201"])
        params[3].float([0.31, 0.33, 0.31])

        stmt.bind_param_batch(params)

        stmt.execute()

        tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(3)
        self._check_select(self.stable_name)

        params = taos.new_bind_params(4)
        params[0].timestamp((1626861392589))
        if dtype == LegalDataType.INT :
            params[1].int((11))
            params[2].int([199])
        elif dtype == LegalDataType.UINT:
            params[1].int_unsigned((11))
            params[2].int_unsigned([199])
        elif dtype == LegalDataType.BIGINT:
            params[1].bigint((11))
            params[2].bigint([199])
        elif dtype == LegalDataType.UBIGINT:
            params[1].bigint_unsigned((11))
            params[2].bigint_unsigned([199])
        elif dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            params[1].binary(("s11"))
            params[2].binary(["s199"])
        params[3].float([0.31])

        stmt.bind_param(params)

        stmt.execute()

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(4)
        self._check_select(self.stable_name)

        params = taos.new_bind_params(4)
        params[0].timestamp((1626861392589))
        if dtype == LegalDataType.INT :
            params[1].int((11))
            params[2].int([1000])
        elif dtype == LegalDataType.UINT:
            params[1].int_unsigned((11))
            params[2].int_unsigned([1000])
        elif dtype == LegalDataType.BIGINT:
            params[1].bigint((11))
            params[2].bigint([1000])
        elif dtype == LegalDataType.UBIGINT:
            params[1].bigint_unsigned((11))
            params[2].bigint_unsigned([1000])
        elif dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            params[1].binary(("s11"))
            params[2].binary(["1000"])
        params[3].float([0.31])

        stmt.bind_param(params)

        stmt.execute()

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(4)
        self._check_select(self.stable_name)

        params = taos.new_bind_params(4)
        params[0].timestamp((1626861392589))
        if dtype == LegalDataType.INT :
            params[1].int(None)
            params[2].int([199])
        elif dtype == LegalDataType.UINT:
            params[1].int_unsigned(None)
            params[2].int_unsigned([199])
        elif dtype == LegalDataType.BIGINT:
            params[1].bigint(None)
            params[2].bigint([199])
        elif dtype == LegalDataType.UBIGINT:
            params[1].bigint_unsigned(None)
            params[2].bigint_unsigned([199])
        elif dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            params[1].binary(None)
            params[2].binary(["s199"])
        params[3].float([0.31])

        try:
            stmt.bind_param(params)
            tdSql.checkEqual(False, True)
        except Exception as errMsg:
            pass

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(4)
        self._check_select(self.stable_name)

        params = taos.new_bind_params(4)
        params[0].timestamp((1626861392589, 1626861392589, ))
        if dtype == LegalDataType.INT :
            params[1].int((None, 18,))
            params[2].int([194, 200])
        elif dtype == LegalDataType.UINT:
            params[1].int_unsigned((None, 18))
            params[2].int_unsigned([194, 200])
        elif dtype == LegalDataType.BIGINT:
            params[1].bigint((None, 18))
            params[2].bigint([194, 200])
        elif dtype == LegalDataType.UBIGINT:
            params[1].bigint_unsigned((None, 18))
            params[2].bigint_unsigned([194, 200])
        elif dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            params[1].binary((None, "s18"))
            params[2].binary(["s194", "s200"])
        params[3].float([0.31, 0.33, 0.31])
        
        try:
            stmt.bind_param(params)
            tdSql.checkEqual(False, True)
        except Exception as errMsg:
            pass
        
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(4)
        self._check_select(self.stable_name)

        if dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            tdSql.query(f'select * from {self.stable_name} where pk="s11"')
            tdSql.checkEqual(tdSql.queryResult[0][2] == '1000', True)
        else:
            tdSql.query(f'select * from {self.stable_name} where pk=11')
            tdSql.checkEqual(tdSql.queryResult[0][2] == 1000, True)
        stmt.close()

    def test_implicit_conversion(self, dtype: LegalDataType):
        
        tdSql.execute(f"drop table if exists dest_table", show=SHOW_LOG)
        tdSql.execute(f"create table dest_table (ts timestamp, pk {dtype.value} primary key, c2 {dtype.value})", show=SHOW_LOG)
        for type in LegalDataType:
            if type == dtype:
                continue
            tdSql.execute(f"drop table if exists source_table", show=SHOW_LOG)
            tdSql.execute(f"create table source_table (ts timestamp, pk {dtype.value} primary key, c2 int)", show=SHOW_LOG)
            tdSql.execute(f"insert into source_table values(now, 100, 1000)", show=SHOW_LOG)
            tdSql.execute(f"insert into dest_table select * from source_table", show=SHOW_LOG)

            tdSql.execute(f"flush database {self.database}", show=SHOW_LOG)
            tdSql.query(f'select * from dest_table where c2=1000')
            tdSql.checkRows(1)
            tdSql.execute(f"delete from dest_table", show=SHOW_LOG)
            self._check_select('dest_table')

    def _compare_table_data(self, result1, result2, row = 0, col = 0):
        for i in range(row):
            for j in range(col):
                if result1[i][j] != result2[i][j]:
                    tdSql.checkEqual(False, True)

    def _check_select(self, table_nam: str):
        tdSql.query(f'select count(*) from {table_nam} ')
        tdSql.query(f'select * from {table_nam}')
        tdSql.query(f'select last_row(*) from {table_nam}')
        tdSql.query(f'select last_row(*) from {table_nam} group by tbname')
        tdSql.query(f'select first(*) from {table_nam}')
        tdSql.query(f'select first(*) from {table_nam} group by tbname')
        tdSql.query(f'select last(*) from {table_nam}')
        tdSql.query(f'select last(*) from {table_nam} group by tbname')
        tdSql.query(f'select ts, last(pk) from {table_nam} order by ts')
        
        tdSql.query(f'select * from {table_nam} order by ts asc')
        tdSql.query(f'select * from {table_nam} order by ts desc')
        tdSql.query(f'select * from {table_nam} order by pk asc')
        tdSql.query(f'select * from {table_nam} order by pk desc')
        tdSql.query(f'select * from {table_nam} order by ts asc, pk desc')
        tdSql.query(f'select * from {table_nam} order by ts desc, pk asc')

    def run(self):  
        tdSql.prepare(replica = self.replicaVar)
        self.prepare_db()

        for date_type in LegalDataType.__members__.items():
            tdLog.info(f'<dateType={date_type}>')
            # 1.insert into value with pk - pass
            tdLog.info('[1.insert into value with pk]')
            self.test_insert_data(date_type[1], HasPK.YES)

            # 2.insert into value without pk - pass
            tdLog.info('[2.insert into value without pk]')
            self.test_insert_data(date_type[1], HasPK.NO)

            # 3.insert into illegal data - pass
            tdLog.info('[3.insert into illegal data]')
            for illegal_data in IllegalData.__members__.items():
                self.test_insert_data_illegal(date_type[1], illegal_data[1])

            # 4. insert into select - pass
            tdLog.info('[4. insert into select]')
            self.test_insert_select(date_type[1])

            # 5. insert into values special cases  - pass
            tdLog.info('[5. insert into values special cases]')
            self.test_insert_values_special(date_type[1])
            
            # 6. test implicit conversion  - pass
            tdLog.info('[6. test implicit conversion]')
            self.test_implicit_conversion(date_type[1])

            # 7. insert into value to mutiple tables  - pass
            tdLog.info('[7. insert into value to mutiple tables]')
            self.test_insert_into_mutiple_tables(date_type[1])

            # 8. stmt  wait for test!!!!
            tdLog.info('[8. stmt  wait for test]')
            self.test_stmt(date_type[1])  
        
        # 9. insert data by schemaless model is not allowed - pass
        tdLog.info('[9. insert data by schemaless model is not allowed]')
        self.test_schemaless_error()
        # while(True):
        #     self.test_stmt(LegalDataType.VARCHAR) 

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
