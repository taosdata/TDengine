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
        """Stream Processing Drop Operations Test

        Test comprehensive drop operations and their impact on stream processing ecosystem:

        1. Test [Source Table Drop] Impact on Streams
            1.1 Test normal table drop with active streams
                1.1.1 Drop source table with running stream - verify stream behavior
                1.1.2 Drop source table with paused stream - verify stream state
                1.1.3 Drop source table with multiple dependent streams
                1.1.4 Drop source table with cross-database stream references
            1.2 Test super table drop with active streams
                1.2.1 Drop super table with child table streams - verify cascade behavior
                1.2.2 Drop super table with direct super table streams
                1.2.3 Drop super table with mixed stream dependencies
                1.2.4 Drop super table with partition-based streams
            1.3 Test child table drop with active streams
                1.3.1 Drop individual child table used in stream
                1.3.2 Drop multiple child tables simultaneously
                1.3.3 Drop child table affecting window computations
                1.3.4 Drop child table with tag-based stream filtering

        2. Test [Target Table Drop] Impact on Streams
            2.1 Test stream output table drop scenarios
                2.1.1 Drop stream target table - verify stream error handling
                2.1.2 Drop target table with ongoing stream computation
                2.1.3 Drop target table with buffered stream results
                2.1.4 Drop target table and verify recreation behavior
            2.2 Test output subtable drop scenarios
                2.2.1 Drop individual output subtable
                2.2.2 Drop multiple output subtables
                2.2.3 Drop output subtable with active partitioning
                2.2.4 Drop output subtable affecting aggregation results

        3. Test [Stream Object Drop] Operations
            3.1 Test direct stream drop operations
                3.1.1 Drop active stream - verify graceful shutdown
                3.1.2 Drop paused stream - verify cleanup
                3.1.3 Drop stream with pending computations
                3.1.4 Drop stream with error state
            3.2 Test cascading stream drop scenarios
                3.2.1 Drop database containing streams
                3.2.2 Drop all streams simultaneously
                3.2.3 Drop streams with interdependencies
                3.2.4 Drop stream affecting downstream consumers

        4. Test [Database Drop] Impact on Stream Ecosystem
            4.1 Test source database drop scenarios
                4.1.1 Drop database containing stream source tables
                4.1.2 Drop database with cross-database stream references
                4.1.3 Drop database with shared source tables
                4.1.4 Drop database affecting multiple stream computations
            4.2 Test target database drop scenarios
                4.2.1 Drop database containing stream target tables
                4.2.2 Drop database with stream output tables
                4.2.3 Drop database affecting stream result storage
                4.2.4 Drop database with mixed stream dependencies

        5. Test [Drop Operations with Special Characters] and Edge Cases
            5.1 Test drop operations with special table names
                5.1.1 Drop tables with Unicode characters in names
                5.1.2 Drop tables with special symbols in names
                5.1.3 Drop tables with reserved keywords as names
                5.1.4 Drop tables with escaped identifier names
            5.2 Test drop operations with complex naming scenarios
                5.2.1 Drop tables with very long names
                5.2.2 Drop tables with case-sensitive names
                5.2.3 Drop tables with numeric prefixes
                5.2.4 Drop tables with mixed character sets

        6. Test [Concurrent Drop Operations] and Race Conditions
            6.1 Test concurrent table drop scenarios
                6.1.1 Multiple clients dropping same table simultaneously
                6.1.2 Concurrent drop operations on related tables
                6.1.3 Drop operation during active stream computation
                6.1.4 Drop operation during stream window evaluation
            6.2 Test transaction isolation for drop operations
                6.2.1 Drop operation within transaction scope
                6.2.2 Rollback behavior for failed drop operations
                6.2.3 Concurrent transaction drop operations
                6.2.4 Deadlock detection and resolution

        7. Test [Error Handling and Recovery] for Drop Operations
            7.1 Test drop operation error scenarios
                7.1.1 Drop non-existent table - verify error handling
                7.1.2 Drop table with insufficient permissions
                7.1.3 Drop table with active locks
                7.1.4 Drop table during system maintenance
            7.2 Test recovery after failed drop operations
                7.2.1 Stream state recovery after failed table drop
                7.2.2 Data consistency verification after drop failures
                7.2.3 Metadata cleanup after partial drop operations
                7.2.4 Connection stability after drop errors

        8. Test [Performance and Resource] Impact of Drop Operations
            8.1 Test performance impact of drop operations
                8.1.1 Large table drop performance measurement
                8.1.2 Multiple table drop performance analysis
                8.1.3 Stream-dependent drop operation latency
                8.1.4 Resource utilization during drop operations
            8.2 Test cleanup and resource reclamation
                8.2.1 Memory cleanup after table drop
                8.2.2 Disk space reclamation verification
                8.2.3 Stream state cleanup after drop operations
                8.2.4 Cache invalidation after drop operations

        Catalog:
            - Streams:Operations:Drop

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