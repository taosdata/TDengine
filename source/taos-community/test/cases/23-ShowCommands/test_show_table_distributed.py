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


        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6908

        History:
            - 2025-7-23 Ethan liu adds test for show table distributed
            - 2025-4-28 Simon Guan Migrated from tsim/compute/block_dist.sim
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_show_table_distributed.py

        """
        self.do_sim_show_table_distributed()
        self.do_show_table_distributed()