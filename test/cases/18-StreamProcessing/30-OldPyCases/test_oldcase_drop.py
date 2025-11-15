import time
import platform
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdCom,
)


class TestStreamDrop:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_drop(self):
        """OldPy: drop operations

        Test drop operations on tables with special characters and batch drop operations:

        1. Test [Normal Table Drop] Operations
            1.1 Create and drop normal table with timestamp column
                1.1.1 Create table with 20 child tables using super table
                1.1.2 Insert 10 rows per child table
                1.1.3 Query and verify data by timestamp column
                1.1.4 Drop each child table individually
            1.2 Test drop with flush database operations
                1.2.1 Recreate child tables after drop
                1.2.2 Insert data again and query
                1.2.3 Verify data consistency after flush
                1.2.4 Drop super table and recreate

        2. Test [Special Character Table Names] Drop Operations
            2.1 Create databases and tables with special names
                2.1.1 Create databases: dbtest_0, dbtest_1 with vgroups 4
                2.1.2 Create super table with Unicode name: aa\u00bf\u200bstb0
                2.1.3 Create child tables with special names: aa\u00bf\u200bctb0, aa\u00bf\u200bctb1
                2.1.4 Create normal tables with special names: aa\u00bf\u200bntb0, aa\u00bf\u200bntb1
            2.2 Test drop operations with special character handling
                2.2.1 Insert data into tables with special names
                2.2.2 Query data using backticks for table names
                2.2.3 Drop tables with special character names
                2.2.4 Clean up databases after testing

        3. Test [Batch Drop Operations] with Super Tables
            3.1 Query information_schema.ins_stables for batch operations
                3.1.1 Find stable information across test databases
                3.1.2 Verify stable count equals 2 (one per database)
                3.1.3 Extract database and stable names for batch operations
            3.2 Test batch drop with error scenarios
                3.2.1 Test "drop table with" invalid table names (should error)
                3.2.2 Test "drop stable with" non-existent tables (should error)
                3.2.3 Test "drop stable with" space-containing names (should error)
                3.2.4 Verify error message: "Cannot drop super table in batch"

        4. Test [Error Handling] for Drop Operations
            4.1 Test error messages for invalid drop operations
                4.1.1 "Table does not exist" for invalid table drop
                4.1.2 "STable not exist" for invalid stable drop
                4.1.3 "Cannot drop super table in batch" for batch stable drop
            4.2 Repeat error tests multiple times
                4.2.1 Execute each error scenario 5 times
                4.2.2 Verify consistent error handling
                4.2.3 Check error message consistency
                4.2.4 Validate connection stability after errors

        Catalog:
            - Streams:OldPyCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-07-22 Beryl Migrated to new test framework
            - Note: removed TSMA tests (not supported in new framework)

        """

        # Initialize variables
        self.dbname = 'db'
        self.ntbname = f"{self.dbname}.ntb"
        self.rowNum = 10
        self.tbnum = 20
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.column_dict = {
            'ts'  : 'timestamp',
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
        self.db_names = ['dbtest_0', 'dbtest_1']
        self.stb_names = ['aa\u00bf\u200bstb0']
        self.ctb_names = ['ctb0', 'ctb1', 'aa\u00bf\u200bctb0', 'aa\u00bf\u200bctb1']
        self.ntb_names = ['ntb0', 'aa\u00bf\u200bntb0', 'ntb1', 'aa\u00bf\u200bntb1']
        self.vgroups_opt = 'vgroups 4'
        self.err_dup_cnt = 5
        self.replicaVar = 1

        # Create a simple setsql object
        class SetSql:
            def set_create_normaltable_sql(self, tbname, column_dict):
                cols = []
                for k, v in column_dict.items():
                    cols.append(f"{k} {v}")
                return f"create table {tbname} ({', '.join(cols)})"
            
            def set_create_stable_sql(self, stbname, column_dict, tag_dict):
                cols = []
                for k, v in column_dict.items():
                    cols.append(f"{k} {v}")
                tags = []
                for k, v in tag_dict.items():
                    tags.append(f"{k} {v}")
                return f"create table {stbname} ({', '.join(cols)}) tags ({', '.join(tags)})"
            
            def set_insertsql(self, column_dict, tbname, binary_str, nchar_str):
                return f"insert into {tbname} values"
            
            def insert_values(self, column_dict, i, insert_sql, insert_list, ts):
                values = [str(ts + i * 1000)]
                for k, v in column_dict.items():
                    if k == 'ts':
                        continue
                    elif 'binary' in v:
                        values.append(f"'binary{i}'")
                    elif 'nchar' in v:
                        values.append(f"'nchar{i}'")
                    elif 'bool' in v:
                        values.append('true')
                    else:
                        values.append(str(i))
                insert_list.append(f" ({', '.join(values)})")
                tdSql.execute(f"{insert_sql} {', '.join(insert_list)}")
                insert_list.clear()

        self.setsql = SetSql()

        self.table_name_with_star()
        self.drop_ntb_check()
        self.drop_stb_ctb_check()
        self.drop_stable_with_check()
        self.drop_table_with_check()
        self.drop_topic_check()
        if platform.system().lower() != 'windows':
            self.drop_stream_check()
        

    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts) 
            
    def drop_ntb_check(self):
        tdSql.execute(f'create database if not exists {self.dbname} replica {self.replicaVar} wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        for k,v in self.column_dict.items():
            if v.lower() == "timestamp":
                tdSql.query(f'select * from {self.ntbname} where {k} = {self.ts}')
                tdSql.checkRows(1)
        tdSql.execute(f'drop table {self.ntbname}')
        tdSql.execute(f'flush database {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        for k,v in self.column_dict.items():
            if v.lower() == "timestamp":
                tdSql.query(f'select * from {self.ntbname} where {k} = {self.ts}')
                tdSql.checkRows(1)
        tdSql.execute(f'drop database {self.dbname}')
    
    def drop_stb_ctb_check(self):
        stbname = f'{self.dbname}.{tdCom.getLongName(5,"letters")}'
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {self.dbname} replica {self.replicaVar} wal_retention_period 3600")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,self.column_dict,tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',self.rowNum)
        for k,v in self.column_dict.items():
            for i in range(self.tbnum):
                if v.lower() == "timestamp":
                    tdSql.query(f'select * from {stbname}_{i} where {k} = {self.ts}')
                    tdSql.checkRows(1)
                    tdSql.execute(f'drop table {stbname}_{i}')
        tdSql.execute(f'flush database {self.dbname}')
        for i in range(self.tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',self.rowNum)
        for k,v in self.column_dict.items():
            for i in range(self.tbnum):
                if v.lower() == "timestamp":
                    tdSql.query(f'select * from {stbname}_{i} where {k} = {self.ts}')
                    tdSql.checkRows(1)
            if v.lower() == "timestamp":
                tdSql.query(f'select * from {stbname} where {k} = {self.ts}')
                tdSql.checkRows(self.tbnum) 
        tdSql.execute(f'drop table {stbname}')
        tdSql.execute(f'flush database {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,self.column_dict,tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',self.rowNum)
        for k,v in self.column_dict.items():
            if v.lower() == "timestamp":
                tdSql.query(f'select * from {stbname} where {k} = {self.ts}')
                tdSql.checkRows(self.tbnum) 
        tdSql.execute(f'drop database {self.dbname}')

    def drop_table_check_init(self):
        for db_name in self.db_names:
            tdSql.execute(f'create database if not exists {db_name} {self.vgroups_opt}')
            tdSql.execute(f'use {db_name}')
            for stb_name in self.stb_names:
                tdSql.execute(f'create table `{stb_name}` (ts timestamp,c0 int) tags(t0 int)')
                for ctb_name in self.ctb_names:
                    tdSql.execute(f'create table `{ctb_name}` using `{stb_name}` tags(0)')
                    tdSql.execute(f'insert into `{ctb_name}` values (now,1)')
            for ntb_name in self.ntb_names:
                tdSql.execute(f'create table `{ntb_name}` (ts timestamp,c0 int)')
                tdSql.execute(f'insert into `{ntb_name}` values (now,1)')

    def drop_table_check_end(self):
        for db_name in self.db_names:
            tdSql.execute(f'drop database {db_name}')

    def drop_stable_with_check(self):
        self.drop_table_check_init()
        tdSql.query(f'select * from information_schema.ins_stables where db_name like "dbtest_%"')
        result = tdSql.queryResult
        print(result)
        tdSql.checkEqual(len(result),2)
        i = 0
        for stb_result in result:
            if i == 0:
                dropTable = f'drop table with `{stb_result[1]}`.`{stb_result[10]}`,'
                dropStable = f'drop stable with `{stb_result[1]}`.`{stb_result[10]}`,'
                dropTableWithSpace = f'drop table with `{stb_result[1]}`.`{stb_result[10]} `,'
                dropStableWithSpace = f'drop stable with `{stb_result[1]}`.` {stb_result[10]}`,'
                dropStableNotExist = f'drop stable with `{stb_result[1]}`.`{stb_result[10]}_notexist`,'
                for _ in range(self.err_dup_cnt):
                    tdLog.info(dropTableWithSpace[:-1])
                    tdSql.error(dropTableWithSpace[:-1], expectErrInfo="Table does not exist", fullMatched=False)
                    tdLog.info(dropStableWithSpace[:-1])
                    tdSql.error(dropStableWithSpace[:-1], expectErrInfo="STable not exist", fullMatched=False)
                    tdLog.info(dropStableNotExist[:-1])
                    tdSql.error(dropStableWithSpace[:-1], expectErrInfo="STable not exist", fullMatched=False)
            else:
                dropTable += f'`{stb_result[1]}`.`{stb_result[10]}`,'
                dropStable += f'`{stb_result[1]}`.`{stb_result[10]}`,'
                for _ in range(self.err_dup_cnt):
                    tdLog.info(dropTable[:-1])
                    tdLog.info(dropStable[:-1])
                    tdSql.error(dropTable[:-1], expectErrInfo="Cannot drop super table in batch")
                    tdSql.error(dropStable[:-1], expectErrInfo="syntax error", fullMatched=False)
                dropTableWithSpace += f'`{stb_result[1]}`.` {stb_result[10]}`,'
                dropStableWithSpace += f'`{stb_result[1]}`.`{stb_result[10]} `,'
                for _ in range(self.err_dup_cnt):
                    tdLog.info(dropTableWithSpace[:-1])
                    tdLog.info(dropStableWithSpace[:-1])
                    tdSql.error(dropTableWithSpace[:-1], expectErrInfo="Table does not exist", fullMatched=False)
                    tdSql.error(dropStableWithSpace[:-1], expectErrInfo="syntax error", fullMatched=False)
            i += 1
        i = 0
        for stb_result in result:
            if i == 0:
                tdSql.execute(f'drop table with `{stb_result[1]}`.`{stb_result[10]}`')
            else:
                tdSql.execute(f'drop stable with `{stb_result[1]}`.`{stb_result[10]}`')
            i += 1
        for i in range(30):
            tdSql.query(f'select * from information_schema.ins_stables where db_name like "dbtest_%"')
            if(len(tdSql.queryResult) == 0):
                break
            tdLog.info(f'ins_stables not empty, sleep 1s')
            time.sleep(1)
        tdSql.query(f'select * from information_schema.ins_stables where db_name like "dbtest_%"')
        tdSql.checkRows(0)
        tdSql.query(f'select * from information_schema.ins_tables where db_name like "dbtest_%"')
        tdSql.checkRows(8)
        for _ in range(self.err_dup_cnt):
            tdSql.error(f'drop stable with information_schema.`ins_tables`;', expectErrInfo="Cannot drop table of system database", fullMatched=False)
            tdSql.error(f'drop stable with performance_schema.`perf_connections`;', expectErrInfo="Cannot drop table of system database", fullMatched=False)
        self.drop_table_check_end()

    def drop_table_with_check(self):
        self.drop_table_check_init()
        tdSql.query(f'select * from information_schema.ins_tables where db_name like "dbtest_%"')
        result = tdSql.queryResult
        print(result)
        tdSql.checkEqual(len(result),16)
        dropTable = f'drop table with '
        for tb_result in result:
            dropTable += f'`{tb_result[1]}`.`{tb_result[5]}`,'
        tdLog.info(dropTable[:-1])
        tdSql.execute(dropTable[:-1])
        for i in range(30):
            tdSql.query(f'select * from information_schema.ins_tables where db_name like "dbtest_%"')
            if(len(tdSql.queryResult) == 0):
                break
            tdLog.info(f'ins_tables not empty, sleep 1s')
            time.sleep(1)
        tdSql.query(f'select * from information_schema.ins_tables where db_name like "dbtest_%"')
        tdSql.checkRows(0)
        tdSql.query(f'select * from information_schema.ins_stables where db_name like "dbtest_%"')
        tdSql.checkRows(2)
        for _ in range(self.err_dup_cnt):
            tdSql.error(f'drop table with information_schema.`ins_tables`;', expectErrInfo="Cannot drop table of system database", fullMatched=False)
            tdSql.error(f'drop table with performance_schema.`perf_connections`;', expectErrInfo="Cannot drop table of system database", fullMatched=False)
        self.drop_table_check_end()

    def drop_topic_check(self):
        tdSql.execute(f'create database {self.dbname} replica {self.replicaVar} wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        stbname = tdCom.getLongName(5,"letters")
        topic_name = tdCom.getLongName(5,"letters")
        tdSql.execute(f'create table {stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create topic {topic_name} as select * from {self.dbname}.{stbname}')
        tdSql.query(f'select * from information_schema.ins_topics where topic_name = "{topic_name}"')
        tdSql.checkEqual(tdSql.queryResult[0][3],f'create topic {topic_name} as select * from {self.dbname}.{stbname}')
        tdSql.execute(f'drop topic {topic_name}')
        tdSql.execute(f'create topic {topic_name} as select c0 from {self.dbname}.{stbname}')
        tdSql.query(f'select * from information_schema.ins_topics where topic_name = "{topic_name}"')
        tdSql.checkEqual(tdSql.queryResult[0][3],f'create topic {topic_name} as select c0 from {self.dbname}.{stbname}')
        tdSql.execute(f'drop topic {topic_name}')

        #TD-25222
        long_topic_name="hhhhjjhhhhqwertyuiasdfghjklzxcvbnmhhhhjjhhhhqwertyuiasdfghjklzxcvbnmhhhhjjhhhhqwertyuiasdfghjklzxcvbnm"
        tdSql.execute(f'create topic {long_topic_name} as select * from {self.dbname}.{stbname}')
        tdSql.execute(f'drop topic {long_topic_name}')

        tdSql.execute(f'drop database {self.dbname}')

    def drop_stream_check(self):
        tdSql.execute(f'create database {self.dbname} replica 1 wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        stbname = tdCom.getLongName(5,"letters")
        stream_name = tdCom.getLongName(5,"letters")
        tdSql.execute(f'create table {stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create table tb using {stbname} tags(1)')
        tdSql.execute(f'create snode on dnode 1')
        tdSql.execute(f'create stream {self.dbname}.{stream_name} interval(10s) sliding(10s) from {self.dbname}.{stbname} partition by tbname into stb as select * from {self.dbname}.{stbname}')
        tdSql.execute(f'drop stream {self.dbname}.{stream_name}')
        time.sleep(5)

        tdSql.execute(f'create stream {self.dbname}.{stream_name} interval(10s) sliding(10s) from {self.dbname}.{stbname} partition by tbname into stb as select * from {self.dbname}.{stbname} ')

    def table_name_with_star(self):
        dbname = "test_tbname_with_star"
        tbname = 's_*cszl01_207602da'
        tdSql.execute(f'create database {dbname} replica 1 wal_retention_period 3600')
        tdSql.execute(f'create table {dbname}.`{tbname}` (ts timestamp, c1 int)', queryTimes=1, show=1)
        tdSql.execute(f"drop table {dbname}.`{tbname}`")
        tdSql.execute(f"drop database {dbname}")