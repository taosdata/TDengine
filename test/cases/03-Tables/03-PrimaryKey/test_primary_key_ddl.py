# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from enum import Enum
from new_test_framework.utils import tdLog, tdSql
import os
import time


class IllegalDataType(Enum):
    NULL       = 'null'
    BOOL       = 'bool'
    TINYINT    = 'tinyint'   
    SMALLINT   = 'smallint'   
    FLOAT      = 'float'
    DOUBLE     = 'double'
    NCHAR      = 'nchar(100)'
    UTINYINT   = 'tinyint unsigned'
    USMALLINT  = 'smallint unsigned'  
    JSON       = 'json'  
    VARBINARY  = 'varbinary(100)'  
    DECIMAL    = 'decimal'  
    BLOB       = 'blob'  
    MEDIUMBLOB = 'mediumblob' 
    GEOMETRY   = 'geometry(512)'  
    EMPTY      = '\'\''
    SPACE      = '\' \''


class LegalDataType(Enum):  
    INT        = 'INT'
    UINT       = 'INT UNSIGNED'
    BIGINT     = 'BIGINT' 
    UBIGINT    = 'BIGINT UNSIGNED'  
    VARCHAR    = 'VARCHAR(100)' 
    BINARY     = 'BINARY(100)'   
    TIMESTAMP  = 'TIMESTAMP'


class LegalSpell(Enum):  
    CASE1      = 'primary key'
    CASE2      = 'PRIMARY KEY'
    CASE3      = 'Primary Key'
    CASE4      = 'PriMary     keY'


class IllegalSpell(Enum):  
    CASE1      = 'primary'
    CASE2      = 'key'
    CASE3      = 'primay key'
    CASE4      = 'primary ky'
    CASE5      = 'primarykey'
    CASE6      = 'key primary'
    CASE7      = 'primay key primay key'


class TableType(Enum):  
    SUPERTABLE     = 0 
    CHILDTABLE     = 1 
    NORNALTABLE     = 2


SHOW_LOG = True


class LegalDataTypeDelete(Enum):  
    INT        = 'INT'
    UINT       = 'INT UNSIGNED'
    BIGINT     = 'BIGINT' 
    UBIGINT    = 'BIGINT UNSIGNED'  
    VARCHAR    = 'VARCHAR(100)' 
    BINARY     = 'BINARY(100)'


class TableTypeDelete(Enum):  
    SUPERTABLE     = 0 
    CHILDTABLE     = 1 
    NORNALTABLE    = 2


STAET_TS = '2023-10-01 00:00:00.000'


class TestCompositePrimaryKeyDdl:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.testcasePath = os.path.split(__file__)[0]
        cls.testcasePath = cls.testcasePath.replace('\\', '//')
        cls.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (cls.testcasePath, cls.testcaseFilename))
        cls.stable_name = 's_table'
        cls.ctable_name = 'c_table'
        cls.ntable_name = 'n_table'
        cls.ts_list = {}

    def setup_method(self):
        # These methods used to run in their own taosd process, so every one of
        # them started from an empty server. They now share a single deployment,
        # therefore drop every user database first to keep that isolation
        # (several cases assert on information_schema.ins_databases).
        tdSql.query(
            "select name from information_schema.ins_databases "
            "where name not in ('information_schema', 'performance_schema')"
        )
        for i in range(tdSql.queryRows):
            tdSql.execute(f"drop database if exists `{tdSql.getData(i, 0)}`")

        #tdSql.init(conn.cursor(), logSql), True)

    def prepare_db(self):
        self.database = "db_create_composite_primary_key"
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database}")
        tdSql.execute(f"use {self.database}")


    def check_pk_definition(self, table_name: str, d_type: LegalDataType, t_type: TableType):
        tdSql.query(f"describe {table_name}", show=SHOW_LOG)
        tdSql.checkData(1, 3, "COMPOSITE KEY")

        if d_type == LegalDataType.BINARY:
            d_type = LegalDataType.VARCHAR

        if t_type == TableType.SUPERTABLE:
            expected_value = f"CREATE STABLE `{table_name}` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `pk` {d_type.value} ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' COMPOSITE KEY, `c2` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`engine` INT)"
        elif t_type == TableType.NORNALTABLE:
            expected_value = f"CREATE TABLE `{table_name}` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `pk` {d_type.value} ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' COMPOSITE KEY, `c2` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium')"

        tdSql.query(f"show create table {table_name}", show=SHOW_LOG)
        result = tdSql.queryResult

        tdSql.checkEqual("COMPOSITE KEY" in result[0][1], True)


    def check_pk_datatype_legal(self, stable_name: str, ctable_name: str, ntable_name: str, dtype: LegalDataType):
        # create super table and child table
        tdSql.execute(f"drop table if exists {stable_name}", show=SHOW_LOG)
        tdSql.execute(f"drop table if exists {ntable_name}", show=SHOW_LOG)
        
        tdSql.execute(f"create table {stable_name} (ts timestamp, pk {dtype.value} COMPOSITE key, c2 int) tags (engine int)", show=SHOW_LOG)
        self.check_pk_definition(stable_name, dtype, TableType.SUPERTABLE)

        tdSql.execute(f"create table {ctable_name} using {stable_name} tags (0)", show=SHOW_LOG)
        tdSql.execute(f"create table {ctable_name}_1 using {stable_name} tags (1) {ctable_name}_2 using {stable_name} tags (2) {ctable_name}_3 using {stable_name} tags (3)", show=SHOW_LOG)

        # create normal table
        tdSql.execute(f"create table {ntable_name} (ts timestamp, pk {dtype.value} COMPOSITE key, c2 int)", show=SHOW_LOG)
        self.check_pk_definition(ntable_name, dtype, TableType.NORNALTABLE)


    def check_pk_datatype_illegal(self, stable_name: str, ntable_name: str, dtype: LegalDataType):
        # create super table and child table
        tdSql.execute(f"drop table if exists {stable_name}", show=SHOW_LOG)
        tdSql.execute(f"drop table if exists {ntable_name}", show=SHOW_LOG)
        
        tdSql.error(f"create table {stable_name} (ts timestamp, pk {dtype.value} primary key, c2 int) tags (engine int)", show=SHOW_LOG)

        # create normal table
        tdSql.error(f"create table {ntable_name} (ts timestamp, pk {dtype.value} primary key, c2 int)", show=SHOW_LOG)


    def check_pk_spell_legal(self, stable_name: str, ntable_name: str, pk_spell: LegalSpell):
        # create super table and child table
        tdSql.execute(f"drop table if exists {stable_name}", show=SHOW_LOG)
        tdSql.execute(f"drop table if exists {ntable_name}", show=SHOW_LOG)
        
        tdSql.execute(f"create table {stable_name} (ts timestamp, pk int {pk_spell.value}, c2 int) tags (engine int)", show=SHOW_LOG)

        # create normal table
        tdSql.execute(f"create table {ntable_name} (ts timestamp, pk int {pk_spell.value}, c2 int)", show=SHOW_LOG)


    def check_pk_spell_illegal(self, stable_name: str, ntable_name: str, pk_spell: LegalSpell):
        # create super table and child table
        tdSql.execute(f"drop table if exists {stable_name}", show=SHOW_LOG)
        tdSql.execute(f"drop table if exists {ntable_name}", show=SHOW_LOG)
        
        tdSql.error(f"create table {stable_name} (ts timestamp, pk int {pk_spell.value}, c2 int) tags (engine int)", show=SHOW_LOG)

        # create normal table
        tdSql.error(f"create table {ntable_name} (ts timestamp, pk int {pk_spell.value}, c2 int)", show=SHOW_LOG)


    def check_update_pk(self, table_name: str, t_type: TableType):
        # create super table and child table
        tdSql.execute(f"drop table if exists {table_name}", show=SHOW_LOG)
        
        if t_type == TableType.SUPERTABLE:
            tdSql.execute(f"create table {table_name} (ts timestamp, c2 int) tags (engine int)", show=SHOW_LOG)
        elif t_type == TableType.NORNALTABLE:
            # create normal table
            tdSql.execute(f"create table {table_name} (ts timestamp, c2 int)", show=SHOW_LOG)

        tdSql.error(f"alter table {table_name} add column pk varchar(100) primary key", show=SHOW_LOG)

        tdSql.execute(f"drop table if exists {table_name}", show=SHOW_LOG)
        if t_type == TableType.SUPERTABLE:
            tdSql.execute(f"create table {table_name} (ts timestamp, pk varchar(200) primary key, c2 int) tags (engine int)", show=SHOW_LOG)
        elif t_type == TableType.NORNALTABLE:
            # create normal table
            tdSql.execute(f"create table {table_name} (ts timestamp, pk varchar(200) primary key, c2 int)", show=SHOW_LOG)

        tdSql.error(f"alter table {table_name} add column new_pk varchar(20) primary key", show=SHOW_LOG)
        for date_type in IllegalDataType.__members__.items():
            tdSql.error(f"alter table {table_name} modify column pk {date_type[1].value}", show=SHOW_LOG)
        for date_type in LegalDataType.__members__.items():
            tdSql.error(f"alter table {table_name} modify column pk {date_type[1].value}", show=SHOW_LOG)

        tdSql.error(f"alter table {table_name} modify column pk varchar(300)", show=SHOW_LOG)
        # renaming the composite primary key column is allowed: key semantics come from the
        # COL_IS_KEY flag and the colId, not from the column name. Rename back so the checks
        # below still address the column as "pk".
        tdSql.execute(f"alter table {table_name} rename column pk new_pk", show=SHOW_LOG)
        tdSql.execute(f"alter table {table_name} rename column new_pk pk", show=SHOW_LOG)
        tdSql.error(f"alter table {table_name} drop column pk", show=SHOW_LOG)


    def test_primary_key_create(self):
        """Composite Primary Key Create

        1. Check legal data type
        2. Check illegal data type
        3. Check legal spell
        4. Check illegal spell
        5. Only define ts and pk columns
        6. Mutiple pk & pk not defined as sencod column
        7. Add/update/delete pk column is not support

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-15 Alex  Duan Migrated from uncatalog/system-test/1-insert/test_composite_primary_key_create.py
            - 2026-11-20 Alex Merged into 03-PrimaryKey/test_primary_key_ddl.py so the methods share one taosd deployment (CI deploys once per test class)

        """
        tdSql.prepare(replica = self.replicaVar)
        self.prepare_db()

        # 1.check legal data type
        for date_type in LegalDataType.__members__.items():
            self.check_pk_datatype_legal('s_table', 'c_table', 'n_table', date_type[1])
        
        # 2.check illegal data type
        for date_type in IllegalDataType.__members__.items():
            self.check_pk_datatype_illegal('s_table', 'n_table', date_type[1])

        # 3.check legal spell
        for date_type in LegalSpell.__members__.items():
            self.check_pk_spell_legal('s_table', 'n_table', date_type[1])

        # 4.check illegal spell
        for date_type in IllegalSpell.__members__.items():
            self.check_pk_spell_illegal('s_table', 'n_table', date_type[1])

        # 5.only define ts and pk columns
        # create super table and child table
        tdSql.execute(f"drop table if exists s_table", show=SHOW_LOG)
        tdSql.execute(f"drop table if exists n_table", show=SHOW_LOG)
        
        tdSql.execute(f"create table s_table (ts timestamp, pk int primary key) tags (engine int)", show=SHOW_LOG)
        tdSql.execute(f"create table c_table using s_table tags (1)", show=SHOW_LOG)
        tdSql.execute(f"insert into c_table values(now, 1)", show=SHOW_LOG)
        tdSql.query(f"select * from s_table", show=SHOW_LOG)
        tdSql.checkRows(1)

        # create normal table
        tdSql.execute(f"create table n_table (ts timestamp, pk int primary key)", show=SHOW_LOG)
        tdSql.execute(f"insert into n_table values(now, 1)", show=SHOW_LOG)
        tdSql.query(f"select * from n_table", show=SHOW_LOG)
        tdSql.checkRows(1)

        # 6.mutiple pk & pk not defined as sencod column
        tdSql.execute(f"drop table if exists s_table", show=SHOW_LOG)
        tdSql.execute(f"drop table if exists n_table", show=SHOW_LOG)

        # create super table 
        tdSql.error(f"create table s_table (ts timestamp, c1 int, pk1 int primary key) tags (engine int)", show=SHOW_LOG)
        tdSql.error(f"create table s_table (ts timestamp, pk1 int primary key, pk2 int primary key) tags (engine int)", show=SHOW_LOG)
        tdSql.error(f"create table s_table (ts timestamp, pk1 int primary key, c2 int, pk2 int primary key) tags (engine int)", show=SHOW_LOG)
        # create normal table
        tdSql.error(f"create table n_table (ts timestamp, c1 int, pk1 int primary key)", show=SHOW_LOG)
        tdSql.error(f"create table n_table (ts timestamp, pk1 int primary key, pk2 int primary key)", show=SHOW_LOG)
        tdSql.error(f"create table n_table (ts timestamp, pk1 int primary key, c2 int, pk2 int primary key)", show=SHOW_LOG)

        # 7.add\update\delete pk column is not support
        self.check_update_pk('s_table', TableType.SUPERTABLE)
        self.check_update_pk('n_table', TableType.NORNALTABLE)



    def prepare_db_delete(self):
        self.database = "db_insert_composite_primary_key"
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database} CACHEMODEL 'none'")
        tdSql.execute(f"use {self.database}")


    def get_latest_ts_delete(self, table_name: str):
        tdSql.query(f'select last(ts) from {table_name}')
        now_time = tdSql.queryResult[0][0].strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"'{now_time}'"

    
    def prepare_data(self, dtype: LegalDataTypeDelete):
        # drop super table and child table
        tdSql.execute(f"drop table if exists {self.stable_name}")
        tdSql.execute(f"drop table if exists {self.ntable_name}")

        # create super table & child table 
        tdSql.execute(f"create table {self.stable_name} (ts timestamp, pk {dtype.value} primary key, c1 smallint, c2 varchar(10)) tags (engine int)", show=SHOW_LOG)

        table_name1 = f'{self.ctable_name}_1'
        tdSql.execute(f"insert into {table_name1} using {self.stable_name} tags(1) values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_1 = self.get_latest_ts_delete(table_name1)
        tdSql.execute(f"insert into {table_name1} values({child_ts_1}, 2, '8', '6') ({child_ts_1}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name1} (ts, pk, c2) values({child_ts_1}, 4, '8') ({child_ts_1}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name1} values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_2 = self.get_latest_ts_delete(table_name1)
        tdSql.execute(f"insert into {table_name1} values({child_ts_2}, 2, '8', '6') ({child_ts_2}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name1} (ts, pk, c2) values({child_ts_2}, 4, '8') ({child_ts_2}, 5, '9')", show=SHOW_LOG)

        table_name2 = f'{self.ctable_name}_2'
        tdSql.execute(f"insert into {table_name2} using {self.stable_name} tags(2) values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_3 = self.get_latest_ts_delete(table_name2)
        tdSql.execute(f"insert into {table_name2} values({child_ts_3}, 2, '8', '6') ({child_ts_3}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name2} (ts, pk, c2) values({child_ts_3}, 4, '8') ({child_ts_3}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name2} values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_4 = self.get_latest_ts_delete(table_name2)
        tdSql.execute(f"insert into {table_name2} values({child_ts_4}, 2, '8', '6') ({child_ts_4}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name2} (ts, pk, c2) values({child_ts_4}, 4, '8') ({child_ts_4}, 5, '9')", show=SHOW_LOG)

        table_name3 = f'{self.ctable_name}_3'
        tdSql.execute(f"insert into {table_name3} using {self.stable_name} tags(3) values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_5 = self.get_latest_ts_delete(table_name3)
        tdSql.execute(f"insert into {table_name3} values({child_ts_5}, 2, '8', '6') ({child_ts_5}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name3} (ts, pk, c2) values({child_ts_5}, 4, '8') ({child_ts_5}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {table_name3} values(now, 1, '7', '6')", show=SHOW_LOG)
        child_ts_6 = self.get_latest_ts_delete(table_name3)
        tdSql.execute(f"insert into {table_name3} values({child_ts_6}, 2, '8', '6') ({child_ts_6}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {table_name3} (ts, pk, c2) values({child_ts_6}, 4, '8') ({child_ts_6}, 5, '9')", show=SHOW_LOG)

        tdSql.query(f'select * from {self.stable_name}')
        tdSql.checkRows(30)

        # create normal table
        tdSql.execute(f"create table {self.ntable_name} (ts timestamp, pk {dtype.value} primary key, c1 smallint, c2 varchar(10))", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name}  values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_1 = self.get_latest_ts_delete(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_1}, 2, '8', '6') ({normal_ts_1}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_1}, 4, '8') ({normal_ts_1}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_2 = self.get_latest_ts_delete(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_2}, 2, '8', '6') ({normal_ts_2}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_2}, 4, '8') ({normal_ts_2}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_3 = self.get_latest_ts_delete(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_3}, 2, '8', '6') ({normal_ts_3}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_3}, 4, '8') ({normal_ts_3}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_4 = self.get_latest_ts_delete(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_4}, 2, '8', '6') ({normal_ts_4}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_4}, 4, '8') ({normal_ts_4}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_5 = self.get_latest_ts_delete(self.ntable_name)
        tdSql.execute(f"insert into {self.ntable_name} values({normal_ts_5}, 2, '8', '6') ({normal_ts_5}, 3, '9', '6')", show=SHOW_LOG)
        tdSql.execute(f"insert into {self.ntable_name} (ts, pk, c2) values({normal_ts_5}, 4, '8') ({normal_ts_5}, 5, '9')", show=SHOW_LOG)

        tdSql.execute(f"insert into {self.ntable_name} values(now, 1, '7', '6')", show=SHOW_LOG)
        normal_ts_6 = self.get_latest_ts_delete(self.ntable_name)
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

    
    def check_delete_data_illegal(self, dtype: LegalDataTypeDelete):
        if dtype == LegalDataTypeDelete.VARCHAR or dtype == LegalDataTypeDelete.BINARY:
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


    def _compare_table_data_delete(self, result1, result2, row = 0, col = 0):
        for i in range(row):
            for j in range(col):
                if result1[i][j] != result2[i][j]:
                    tdSql.checkEqual(False, True)


    def test_primary_key_delete(self):
        """Composite Primary Key Delete

        This test ensures that TDengine's delete operations strictly follow time-series database design principles: only allowing data deletion by time dimension.

        1. Data Preparation
        - Creates super tables and normal tables, both containing composite primary keys (ts + pk columns)
        - Supports multiple data types for primary keys: INT, UINT, BIGINT, UBIGINT, VARCHAR, BINARY
        - Inserts test data into child tables and normal tables

        2. Legal Delete Operation Tests
        - Delete by timestamp: `DELETE FROM table WHERE ts = 'xxx'`
        - Delete by time range: `DELETE FROM table WHERE ts > 'xxx' AND ts < 'yyy'`
        - Delete entire table: `DELETE FROM table`

        3. Illegal Delete Operation Tests
        - Delete by primary key column: `DELETE FROM table WHERE pk = 1`
        - Delete by timestamp + primary key: `DELETE FROM table WHERE ts = 'xxx' AND pk = 1`
        - Delete by time range + primary key: `DELETE FROM table WHERE ts > 'xxx' AND ts < 'yyy' AND pk = 1`

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-15 Alex  Duan Migrated from uncatalog/system-test/1-insert/test_composite_primary_key_delete.py
            - 2026-11-20 Alex Merged into 03-PrimaryKey/test_primary_key_ddl.py so the methods share one taosd deployment (CI deploys once per test class)

        """
        tdSql.prepare(replica = self.replicaVar)
        self.prepare_db_delete()

        for date_type in LegalDataTypeDelete.__members__.items():
            self.prepare_data(date_type[1])
            self.check_delete_data_illegal(date_type[1])
        self.check_delete_data()
