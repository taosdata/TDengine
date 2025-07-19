from new_test_framework.utils import tdLog, tdSql
from itertools import product
import taos
import random
import time
from taos.tmq import *


class TestShowTableDistributed:
    """This test case is used to veirfy show table distributed command"""

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.dbname = "distributed_db"
        cls.stname = "st"
        cls.ctnum = 1
        cls.row_num = 99

        # create database
        tdSql.execute(f'create database if not exists {cls.dbname};')
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
        tdLog.debug("init finished")
    
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

    def test_show_table_distributed(self):
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
        # close the connection
        tdLog.success("%s successfully executed" % __file__)

