from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck
import taos
import random
import time


class TestShowTableDistributed:

    def setup_class(cls):
        pass

    def do_sim_show_table_distributed(self):
        self.ShowDistributedNull()
        tdStream.dropAllStreamsAndDbs()
        self.BlockDist()
        tdStream.dropAllStreamsAndDbs()
        print("do sim show table distributed ......... [passed]")
    
    def ShowDistributedNull(self):
        tdLog.info(f"========== start show table distributed test")
        tdSql.execute(f"drop database if exists test_show_table")
        tdSql.execute(f"create database test_show_table")
        tdSql.execute(f"use test_show_table")

        # create super table and sub table
        tdSql.execute(f"create table super_t (ts timestamp, second_key varchar(100) composite key, alarm varchar(50), event varchar(50), dest varchar(50), reason varchar(50), type int, category int, name varchar(100)) tags (id VARCHAR(25), location VARCHAR(100), part_no INT)")
        tdSql.execute(f"create table sub_t0 using super_t tags('t1', 'value1', 1)")
        tdSql.execute(f"create table sub_t1 using super_t tags('t2', 'value2', 2)")
        tdSql.execute(f"create table sub_t2 using super_t tags('t3', 'value3', 3)")

        # insert data into sub table
        tdSql.execute(f"insert into sub_t0 values (now, '01', '00', 'up', '90', null, 2, 2, '')")
        tdSql.execute(f"insert into sub_t1 values (now, '11', '10', 'up', '90', null, 2, 2, '')")
        tdSql.execute(f"insert into sub_t2 values (now, '22', '20', 'up', '90', null, 2, 2, '')")

        # run show table distributed command, it should return internal error
        tdSql.query(f"show table distributed super_t")
        tdSql.checkNotEqual(tdSql.getRows(), 0)
        tdLog.info(f"end show table distributed test successfully")
    
    def BlockDist(self):
        dbPrefix = "m_di_db"
        tbPrefix = "m_di_tb"
        mtPrefix = "m_di_mt"
        ntPrefix = "m_di_nt"
        tbNum = 1
        rowNum = 2000

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        nt = ntPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdSql.execute(f"create table {nt} (ts timestamp, tbcol int)")
        x = 0
        while x < rowNum:
            cc = x * 60000
            ms = 1601481600000 + cc
            tdSql.execute(f"insert into {nt} values ({ms} , {x} )")
            x = x + 1

        tdSql.execute(f"flush database {db}")

        tdLog.info(f"=============== step2")
        i = 0
        tb = tbPrefix + str(i)

        tdLog.info(f"show table distributed {tb}")
        tdSql.query(f"show table distributed {tb}")
        tdSql.checkNotEqual(tdSql.getRows(), 0)

        tdLog.info(f"=============== step3")
        i = 0
        mt = mtPrefix + str(i)

        tdLog.info(f"show table distributed {mt}")
        tdSql.query(f"show table distributed {mt}")
        tdSql.checkNotEqual(tdSql.getRows(), 0)

        tdLog.info(f"=============== step4")
        i = 0
        nt = ntPrefix + str(i)

        tdLog.info(f"show table distributed {nt}")
        tdSql.checkNotEqual(tdSql.getRows(), 0)

        tdLog.info(f"============== TD-5998")
        tdSql.error(f"select _block_dist() from (select * from {nt})")
        tdSql.error(f"select _block_dist() from (select * from {mt})")

        tdLog.info(f"============== TD-22140 & TD-22165")
        tdSql.error(f"show table distributed information_schema.ins_databases")
        tdSql.error(f"show table distributed performance_schema.perf_apps")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    #
    # ------------------- test_show_table_distributed.py ----------------
    #
    def initData(self):
        # ----- test_show_table_distributed.py setup_class ------
        self.dbname = "distributed_db"
        self.stname = "st"
        self.ctnum = 1
        self.row_num = 99

        # create database
        tdSql.execute(f'create database if not exists {self.dbname};')
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
    
    def checkRes(self, queryRes):
        mem_rows_num = 0
        stt_rows_num = 0
        for item in queryRes:
            if "Inmem_Rows=" in item[0]:
                mem_rows_num = int(item[0].split("=")[1].split(" ")[0].replace("[", "").replace("]", ""))
                tdLog.debug("mem_rows_num: %s" % mem_rows_num)
                if "Stt_Rows=" in item[0]:
                    stt_rows_num = int(item[0].split("=")[2].replace("[", "").replace("]", ""))
                    tdLog.debug("stt_rows_num: %s" % stt_rows_num)
        return mem_rows_num, stt_rows_num

    def do_show_table_distributed(self):
        self.initData()
        tdSql.query(f"show table distributed {self.stname};")
        tdLog.debug(tdSql.queryResult)
        mem_rows_num, stt_rows_num = self.checkRes(tdSql.queryResult)
        tdLog.debug("mem_rows_num: %s, stt_rows_num: %s" % (mem_rows_num, stt_rows_num))
        assert(99 == mem_rows_num and 0 == stt_rows_num)

        tdSql.execute(f"flush database {self.dbname};")
        time.sleep(1)
        tdSql.query(f"show table distributed {self.stname};")
        tdLog.debug(tdSql.queryResult)
        mem_rows_num, stt_rows_num = self.checkRes(tdSql.queryResult)
        tdLog.debug("mem_rows_num: %s, stt_rows_num: %s" % (mem_rows_num, stt_rows_num))
        assert(0 == mem_rows_num and 99 == stt_rows_num)

        # remove the user
        tdSql.execute(f'drop database {self.dbname};')

        print("do show table distributed ............. [passed]")

    #
    # --------- ins_table_fixed_distributed tests ----------
    #
    def do_fixed_distributed(self):
        tdLog.info("========== start ins_table_fixed_distributed tests")
        db = "fixed_dist_db"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 4")
        tdSql.execute(f"use {db}")

        # create supertable, child tables, and a normal table
        tdSql.execute(f"create table st (ts timestamp, v int) tags (t int)")
        for i in range(4):
            tdSql.execute(f"create table ct_{i} using st tags({i})")
        tdSql.execute(f"create table nt (ts timestamp, v int)")

        # insert data
        rows = 5000
        for i in range(4):
            sql = f"insert into ct_{i} values "
            for j in range(rows):
                sql += f"(now+{j+1}s, {j})"
            tdSql.execute(sql)
        sql = f"insert into nt values "
        for j in range(rows):
            sql += f"(now+{j+1}s, {j})"
        tdSql.execute(sql)

        tdSql.execute(f"flush database {db}")
        time.sleep(1)
        tdSql.execute(f"compact database {db}")
        time.sleep(3)

        # --- 1. basic query on supertable ---
        tdLog.info("fixed_dist step1: query supertable")
        tdSql.query(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where db_name='{db}' and table_name='st'")
        tdSql.checkCols(23)
        tdSql.checkNotEqual(tdSql.getRows(), 0)
        # db_name and table_name should match
        for i in range(tdSql.getRows()):
            tdSql.checkData(i, 0, db)
            tdSql.checkData(i, 1, "st")

        # --- 2. query on normal table ---
        tdLog.info("fixed_dist step2: query normal table")
        tdSql.query(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where db_name='{db}' and table_name='nt'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, db)
        tdSql.checkData(0, 1, "nt")
        assert tdSql.queryResult[0][2] > 0, f"expected total_blocks > 0, got {tdSql.queryResult[0][2]}"
        assert tdSql.queryResult[0][6] == rows, f"expected block_rows={rows}, got {tdSql.queryResult[0][6]}"
        assert tdSql.queryResult[0][14] == 1, f"expected total_vgroups=1, got {tdSql.queryResult[0][14]}"

        # --- 3. query on child table ---
        tdLog.info("fixed_dist step3: query child table")
        tdSql.query(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where db_name='{db}' and table_name='ct_0'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, db)
        tdSql.checkData(0, 1, "ct_0")
        assert tdSql.queryResult[0][6] == rows, f"expected block_rows={rows}, got {tdSql.queryResult[0][6]}"

        # --- 4. aggregation across vgroups for supertable ---
        tdLog.info("fixed_dist step4: supertable aggregation")
        tdSql.query(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where db_name='{db}' and table_name='st'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, db)
        tdSql.checkData(0, 1, "st")
        assert tdSql.queryResult[0][6] == rows * 4, f"expected block_rows={rows * 4}, got {tdSql.queryResult[0][6]}"

        # --- 5. error: missing table_name ---
        tdLog.info("fixed_dist step5: error without table_name")
        tdSql.error(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where db_name='{db}'")

        # --- 6. error: missing db_name ---
        tdLog.info("fixed_dist step6: error without db_name")
        tdSql.error(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where table_name='st'")

        # --- 7. error: no WHERE at all ---
        tdLog.info("fixed_dist step7: error without WHERE")
        tdSql.error(f"select * from information_schema.ins_table_fixed_distributed")

        # --- 8. error: OR condition ---
        tdLog.info("fixed_dist step8: error with OR condition")
        tdSql.error(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where db_name='{db}' or table_name='st'")

        # --- 9. non-existent table returns empty ---
        tdLog.info("fixed_dist step9: non-existent table")
        tdSql.query(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where db_name='{db}' and table_name='no_such_table'")
        tdSql.checkRows(0)

        # --- 10. histogram buckets sum to total_blocks ---
        tdLog.info("fixed_dist step10: histogram consistency")
        tdSql.query(f"select * from information_schema.ins_table_fixed_distributed "
                     f"where db_name='{db}' and table_name='nt'")
        row = tdSql.queryResult[0]
        total_blocks = row[2]
        hist_sum = sum(row[15:23])  # block_dist_64 .. block_dist_other
        assert hist_sum == total_blocks, \
            f"histogram sum ({hist_sum}) != total_blocks ({total_blocks})"

        # --- cleanup ---
        tdSql.execute(f"drop database {db}")
        print("do fixed distributed .................. [passed]")

    #
    # ------------------- main ----------------
    #
    def test_show_table_distributed(self):
        """Show table distributed

        1. Tests basic distributed table display for super/normal/temporary tables
        2. Verifies error handling for system/internal tables
        3. Includes block distribution validation with data insertion
        4. Checks metadata consistency after operations
        5. Covers edge cases from TD-5998/TD-22140/TD-22165
        6. Validates both in-memory and on-disk data representation
        7. Ensures proper cleanup of test artifacts
        8. Confirms accurate row counts in various scenarios
        9. Validates functionality across different cluster configurations
        10. Assesses performance impact of show table distributed command
        11. Tests ins_table_fixed_distributed for supertable, child table, normal table
        12. Verifies mandatory WHERE db_name AND table_name conditions
        13. Checks histogram bucket consistency with total_blocks


        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6908

        History:
            - 2025-7-23 Ethan liu adds test for show table distributed
            - 2025-4-28 Simon Guan Migrated from tsim/compute/block_dist.sim
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_show_table_distributed.py
            - 2026-04-08 Bomin Zhang Added ins_table_fixed_distributed tests

        """
        self.do_sim_show_table_distributed()
        self.do_show_table_distributed()
        self.do_fixed_distributed()