import pytest
import logging
import taos
import sys
import datetime
import inspect
import random

logger = logging.getLogger(__name__)

class TestInsertDouble:

    def setup_class(cls):
        cls.database = "db1"
        logger.debug(f"start to excute {__file__}")
        cls.tdSql.prepare(replica = cls.replicaVar)

        cls.tdSql.execute(f"drop database if exists {cls.database}")
        cls.tdSql.execute(f"create database {cls.database} replica {cls.replicaVar}")
        cls.tdSql.execute(f"use {cls.database}")

    def test_value(self):
        """测试插入各种double值

        插入各种double值包括正负值、科学计数法、十六进制、二进制、字符串
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira:

        History:
            - 2024-2-6 Feng Chao Created
            - 2025-2-26 Huo Hong Migrated to new test framework

        """
        check_list = [
            ("t1", "bigint", 64),
            ("t2", "int", 32),
            ("t3", "smallint", 16),
            ("t4", "tinyint", 8)
        ]
        for table_name, dtype, bits in check_list:
            self.tdSql.execute(f"drop table if exists {table_name}")
            self.tdSql.execute(f"create table {table_name}(ts timestamp, i1 {dtype}, i2 {dtype} unsigned)")

            self.tdSql.execute(f"insert into {table_name} values(1717122943000, -16, +6)")
            self.tdSql.execute(f"insert into {table_name} values(1717122944000, 80.99, +0042)")
            self.tdSql.execute(f"insert into {table_name} values(1717122945000, -0042, +80.99)")
            self.tdSql.execute(f"insert into {table_name} values(1717122946000, 52.34354, 18.6)")
            self.tdSql.execute(f"insert into {table_name} values(1717122947000, -12., +3.)")
            self.tdSql.execute(f"insert into {table_name} values(1717122948000, -0.12, +3.0)")
            self.tdSql.execute(f"insert into {table_name} values(1717122949000, -2.3e1, +2.324e2)")
            self.tdSql.execute(f"insert into {table_name} values(1717122950000, -2e1,  +2e2)")
            self.tdSql.execute(f"insert into {table_name} values(1717122951000, -2.e1, +2.e2)")
            self.tdSql.execute(f"insert into {table_name} values(1717122952000, -0x40, +0b10000)")
            self.tdSql.execute(f"insert into {table_name} values(1717122953000, -0b10000, +0x40)")

            # str support
            self.tdSql.execute(f"insert into {table_name} values(1717122954000, '-16', '+6')")
            self.tdSql.execute(f"insert into {table_name} values(1717122955000, ' -80.99', ' +0042')")
            self.tdSql.execute(f"insert into {table_name} values(1717122956000, ' -0042', ' +80.99')")
            self.tdSql.execute(f"insert into {table_name} values(1717122957000, '52.34354', '18.6')")
            self.tdSql.execute(f"insert into {table_name} values(1717122958000, '-12.', '+5.')")
            self.tdSql.execute(f"insert into {table_name} values(1717122959000, '-.12', '+.5')")
            self.tdSql.execute(f"insert into {table_name} values(1717122960000, '-2.e1', '+2.e2')")
            self.tdSql.execute(f"insert into {table_name} values(1717122961000, '-2e1',  '+2e2')")
            self.tdSql.execute(f"insert into {table_name} values(1717122962000, '-2.3e1', '+2.324e2')")
            self.tdSql.execute(f"insert into {table_name} values(1717122963000, '-0x40', '+0b10010')")
            self.tdSql.execute(f"insert into {table_name} values(1717122964000, '-0b10010', '+0x40')")

            self.tdSql.query(f"select * from {table_name}")
            self.tdSql.checkRows(22)

            baseval = 2**(bits/2)
            negval = -baseval + 1.645
            posval = baseval + 4.323
            bigval = 2**(bits-1)
            max_i = bigval - 1
            min_i = -bigval
            max_u = 2*bigval - 1
            min_u = 0
            print("val:", baseval, negval, posval, max_i)

            self.tdSql.execute(f"insert into {table_name} values(1717122965000, {negval}, {posval})")
            self.tdSql.execute(f"insert into {table_name} values(1717122966000, -{baseval}, {baseval})")
            self.tdSql.execute(f"insert into {table_name} values(1717122967000, {max_i}, {max_u})")
            self.tdSql.execute(f"insert into {table_name} values(1717122968000, {min_i}, {min_u})")

            self.tdSql.query(f"select * from {table_name}")
            self.tdSql.checkRows(26)
            
            # error case
            self.tdSql.error(f"insert into {table_name} values(1717122969000, 0, {max_u+1})")
            self.tdSql.error(f"insert into {table_name} values(1717122970000, 0, -1)")
            self.tdSql.error(f"insert into {table_name} values(1717122971000, 0, -2.0)")
            self.tdSql.error(f"insert into {table_name} values(1717122972000, 0, '-2.0')")
            self.tdSql.error(f"insert into {table_name} values(1717122973000, {max_i+1}, 0)")
            self.tdSql.error(f"insert into {table_name} values(1717122974000, {min_i-1}, 0)")
            self.tdSql.error(f"insert into {table_name} values(1717122975000, '{min_i-1}', 0)")
        logger.info("==========end case1 run ...............")
    
    def test_tags(self):
        """测试插入各种double值到tag

        插入各种double值包括正负值、科学计数法、十六进制、二进制、字符串到tag

        Since: v3.0.0.0

        Labels: common,ci

        Jira:

        History:
            - 2024-2-6 Feng Chao Created
            - 2025-2-26 Huo Hong Migrated to new test framework

        """
        check_list = [
            ("t_big", "bigint", 64),
            ("t_int", "int", 32),
            ("t_small", "smallint", 16),
            ("t_tiny", "tinyint", 8)
        ]
        for stable_name, dtype, bits in check_list:
            self.tdSql.execute(f"create stable {stable_name}(ts timestamp, i1 {dtype}, i2 {dtype} unsigned) tags(id {dtype})")

            baseval = 2**(bits/2)
            negval = -baseval + 1.645
            posval = baseval + 4.323
            bigval = 2**(bits-1)
            max_i = bigval - 1
            min_i = -bigval
            max_u = 2*bigval - 1
            min_u = 0

            self.tdSql.execute(f"insert into {stable_name}_1 using {stable_name} tags('{negval}') values(1717122976000, {negval}, {posval})")
            self.tdSql.execute(f"insert into {stable_name}_2 using {stable_name} tags({posval}) values(1717122977000, -{baseval} , {baseval})")
            self.tdSql.execute(f"insert into {stable_name}_3 using {stable_name} tags('0x40') values(1717122978000, {max_i}, {max_u})")
            self.tdSql.execute(f"insert into {stable_name}_4 using {stable_name} tags(0b10000) values(1717122979000, {min_i}, {min_u})")
            
            self.tdSql.execute(f"insert into {stable_name}_5 using {stable_name} tags({max_i}) values(1717122980000, '{negval}', '{posval}')")
            self.tdSql.execute(f"insert into {stable_name}_6 using {stable_name} tags('{min_i}') values(1717122981000, '-{baseval}' , '{baseval}')")
            self.tdSql.execute(f"insert into {stable_name}_7 using {stable_name} tags(-0x40) values(1717122982000, '{max_i}', '{max_u}')")
            self.tdSql.execute(f"insert into {stable_name}_8 using {stable_name} tags('-0b10000') values(1717122983000, '{min_i}', '{min_u}')")

            self.tdSql.execute(f"insert into {stable_name}_9 using {stable_name} tags(12.) values(1717122984000, {negval}, {posval})")
            self.tdSql.execute(f"insert into {stable_name}_10 using {stable_name} tags('-8.3') values(1717122985000, -{baseval} , {baseval})")
            self.tdSql.execute(f"insert into {stable_name}_11 using {stable_name} tags(2.e1) values(1717122986000, {max_i}, {max_u})")
            self.tdSql.execute(f"insert into {stable_name}_12 using {stable_name} tags('-2.3e1') values(1717122987000, {min_i}, {min_u})")

            self.tdSql.query(f"select * from {stable_name}")
            self.tdSql.checkRows(12)
            logger.info("==========end case2 run ...............")

    

    def teardown_class(cls):
        logger.info(f"{__file__} successfully executed")
