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
    TIMESTAMP  = 'timestamp'
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

class TestCompositePrimaryKeyCreate:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.database = "db_create_composite_primary_key"
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql), True)

    def prepare_db(self):
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
        tdSql.error(f"alter table {table_name} rename column pk new_pk", show=SHOW_LOG)
        tdSql.error(f"alter table {table_name} drop column pk", show=SHOW_LOG)

    def test_composite_primary_key_create(self):
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
        
        tdLog.success(f"{__file__} successfully executed")
        
