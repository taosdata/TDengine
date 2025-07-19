from enum import Enum

from new_test_framework.utils import tdLog, tdSql
import os
import time

class LegalDataType(Enum):  
    INT        = 'INT'
    UINT       = 'INT UNSIGNED'
    BIGINT     = 'BIGINT' 
    UBIGINT    = 'BIGINT UNSIGNED'  
    VARCHAR    = 'VARCHAR(100)' 
    BINARY     = 'BINARY(100)'   


class TableType(Enum):  
    SUPERTABLE     = 0 
    CHILDTABLE     = 1 
    NORNALTABLE    = 2 

SHOW_LOG = True
STAET_TS = '2023-10-01 00:00:00.000'

class TestCompositePrimaryKeyDelete:

    def setup_class(cls):
        cls.database = "db_insert_composite_primary_key"
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql), True)

        cls.testcasePath = os.path.split(__file__)[0]
        cls.testcasePath = cls.testcasePath.replace('\\', '//')
        cls.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (cls.testcasePath,cls.testcaseFilename))

        cls.stable_name = 's_table'
        cls.ctable_name = 'c_table'
        cls.ntable_name = 'n_table'
        cls.ts_list = {}


    def prepare_db(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database} CACHEMODEL 'none'")
        tdSql.execute(f"use {self.database}")

    def get_latest_ts(self, table_name: str):
        tdSql.query(f'select last(ts) from {table_name}')
        now_time = tdSql.queryResult[0][0].strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"'{now_time}'"
    
    def prepare_data(self, dtype: LegalDataType):
        # drop super table and child table
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"drop table if exists {self.ntable_name}")

        # create super table & child table 
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c1 smallint, c2 varchar(10)) tags (engine int)", show=SHOW_LOG)

        table_name1 = f'{self.ctable_name}_1'
        tdSql.execute(f"insert into {table_name1} using {self.stable_name} tags(1) values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_1 = self.get_latest_ts(table_name1)
        tdSql.execute(f"insert into {table_name1} values({child_ts_1}, 2, '8', '6') ({child_ts_1}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name1} (ts, pk, c2) values({child_ts_1}, 4, '8') ({child_ts_1}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name1} values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_2 = self.get_latest_ts(table_name1)
        tdSql.execute(f"insert into {table_name1} values({child_ts_2}, 2, '8', '6') ({child_ts_2}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name1} (ts, pk, c2) values({child_ts_2}, 4, '8') ({child_ts_2}, 5, '9')", show=SHOW_LOG)

        table_name2 = f'{self.ctable_name}_2'
        tdSql.execute(f"insert into {table_name2} using {self.stable_name} tags(2) values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_3 = self.get_latest_ts(table_name2)
        tdSql.execute(f"insert into {table_name2} values({child_ts_3}, 2, '8', '6') ({child_ts_3}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name2} (ts, pk, c2) values({child_ts_3}, 4, '8') ({child_ts_3}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name2} values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_4 = self.get_latest_ts(table_name2)
        tdSql.execute(f"insert into {table_name2} values({child_ts_4}, 2, '8', '6') ({child_ts_4}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name2} (ts, pk, c2) values({child_ts_4}, 4, '8') ({child_ts_4}, 5, '9')", show=SHOW_LOG)

        table_name3 = f'{self.ctable_name}_3'
        tdSql.execute(f"insert into {table_name3} using {self.stable_name} tags(3) values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_5 = self.get_latest_ts(table_name3)
        tdSql.execute(f"insert into {table_name3} values({child_ts_5}, 2, '8', '6') ({child_ts_5}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name3} (ts, pk, c2) values({child_ts_5}, 4, '8') ({child_ts_5}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name3} values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_6 = self.get_latest_ts(table_name3)
        tdSql.execute(f"insert into {table_name3} values({child_ts_6}, 2, '8', '6') ({child_ts_6}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name3} (ts, pk, c2) values({child_ts_6}, 4, '8') ({child_ts_6}, 5, '9')", show=SHOW_LOG)

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(30)

        # create normal table
        tdSql.execute(f"create table {self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c1 smallint, c2 varchar(10))", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name}  values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_1 = self.get_latest_ts(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_1}, 2, '8', '6') ({normal_ts_1}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_1}, 4, '8') ({normal_ts_1}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_2 = self.get_latest_ts(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_2}, 2, '8', '6') ({normal_ts_2}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_2}, 4, '8') ({normal_ts_2}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_3 = self.get_latest_ts(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_3}, 2, '8', '6') ({normal_ts_3}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_3}, 4, '8') ({normal_ts_3}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_4 = self.get_latest_ts(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_4}, 2, '8', '6') ({normal_ts_4}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_4}, 4, '8') ({normal_ts_4}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_5 = self.get_latest_ts(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_5}, 2, '8', '6') ({normal_ts_5}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_5}, 4, '8') ({normal_ts_5}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_6 = self.get_latest_ts(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_6}, 2, '8', '6') ({normal_ts_6}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_6}, 4, '8') ({normal_ts_6}, 5, '9')", show=SHOW_LOG)

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(30)
        
        self.ts_list['child_ts_1'] = child_ts_1
        self.ts_list['child_ts_2'] = child_ts_2
        self.ts_list['child_ts_3'] = child_ts_3
        self.ts_list['child_ts_4'] = child_ts_4
        self.ts_list['child_ts_5'] = child_ts_5
        self.ts_list['child_ts_6'] = child_ts_6
        self.ts_list['normal_ts_1'] = normal_ts_1
        self.ts_list['normal_ts_2'] = normal_ts_2
        self.ts_list['normal_ts_3'] = normal_ts_3
        self.ts_list['normal_ts_4'] = normal_ts_4
        self.ts_list['normal_ts_5'] = normal_ts_5
        self.ts_list['normal_ts_6'] = normal_ts_6

        

    def check_delete_data(self):
        # delete with ts
        tdSql.execute(f"delete from {self.stable_name} where ts={self.ts_list['child_ts_1']} ", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(25)
        tdSql.execute(f"delete from {self.ctable_name}_1 where ts={self.ts_list['child_ts_2']} ", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(20)
        tdSql.execute(f"delete from {self.ntable_name} where ts={self.ts_list['normal_ts_1']} ", show=SHOW_LOG)
        tdSql.query(f'select * from {self.ntable_name}')
        tdSql.checkRows(25)

        # delete with ts range
        tdSql.execute(f"delete from {self.stable_name} where ts>{self.ts_list['child_ts_2']} and ts<{self.ts_list['child_ts_4']}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(15)
        tdSql.execute(f"delete from {self.ctable_name}_2 where ts>{self.ts_list['child_ts_2']} and ts<{self.ts_list['child_ts_5']}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(10)
        tdSql.execute(f"delete from {self.ntable_name} where ts>{self.ts_list['normal_ts_2']} and ts<{self.ts_list['normal_ts_5']}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.ntable_name}')
        tdSql.checkRows(15)

        tdSql.execute(f"delete from {self.stable_name}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)

        table_name4 = f'{self.ctable_name}_4'
        tdSql.execute(f"insert into {table_name4} using {self.stable_name} tags(3) values(now, 1, '7', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name4} values(now+1s, 2, '8', '6') (now+2s, 3, '9', '6')", show=SHOW_LOG)

        tdSql.execute(f"delete from {table_name4}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        tdSql.execute(f"delete from {self.ntable_name}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.ntable_name}')
        tdSql.checkRows(0)

        tdSql.execute(f"delete from {self.stable_name}", show=SHOW_LOG)
        tdSql.execute(f"delete from {table_name4}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(0)
        tdSql.execute(f"delete from {self.ntable_name}", show=SHOW_LOG)
        tdSql.query(f'select * from {self.ntable_name}')
        tdSql.checkRows(0)
    
    def check_delete_data_illegal(self, dtype: LegalDataType):
        if dtype == LegalDataType.VARCHAR or dtype == LegalDataType.BINARY:
            pk_condition_value = '\'1\''
        else:
            pk_condition_value = '1'
        # delete with ts & pk
        tdSql.error(f"delete from {self.stable_name} where ts={self.ts_list['child_ts_1']} and pk={pk_condition_value}", show=SHOW_LOG)
        tdSql.error(f"delete from {self.ctable_name}_2 where ts={self.ts_list['child_ts_1']} and pk={pk_condition_value}", show=SHOW_LOG)
        tdSql.error(f"delete from {self.ntable_name} where ts={self.ts_list['normal_ts_1']} and pk={pk_condition_value}", show=SHOW_LOG)

        # delete with ts range & pk
        tdSql.error(f"delete from {self.stable_name} where ts>{self.ts_list['child_ts_1']} and ts<{self.ts_list['child_ts_2']} and pk={pk_condition_value}", show=SHOW_LOG)
        tdSql.error(f"delete from {self.ctable_name}_2 where ts>{self.ts_list['child_ts_1']} and ts<{self.ts_list['child_ts_2']} and pk={pk_condition_value}", show=SHOW_LOG)
        tdSql.error(f"delete from {self.ntable_name} where ts>{self.ts_list['normal_ts_1']} and ts<{self.ts_list['normal_ts_2']} and pk={pk_condition_value}", show=SHOW_LOG)

        # delete with pk
        tdSql.error(f"delete from {self.stable_name} where pk={pk_condition_value}", show=SHOW_LOG)
        tdSql.error(f"delete from {self.ctable_name}_2 where pk={pk_condition_value}", show=SHOW_LOG)
        tdSql.error(f"delete from {self.ntable_name} where pk={pk_condition_value}", show=SHOW_LOG)

    def _compare_table_data(self, result1, result2, row = 0, col = 0):
        for i in range(row):
            for j in range(col):
                if result1[i][j] != result2[i][j]:
                    tdSql.checkEqual(False, True)

    def test_composite_primary_key_delete(self):
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
        tdSql.prepare(replica = self.replicaVar)
        self.prepare_db()

        for date_type in LegalDataType.__members__.items():
            self.prepare_data(date_type[1])
            self.check_delete_data_illegal(date_type[1])
        self.check_delete_data()
        tdLog.success(f"{__file__} successfully executed")
        
