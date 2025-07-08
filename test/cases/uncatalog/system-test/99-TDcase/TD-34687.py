import random
import string

from new_test_framework.utils.log import tdLog
from new_test_framework.utils.sql import tdSql

class TestTD_34687:

    def setup_class(cls):
        random.seed(1) # for reproducibility
        tdLog.debug(f"start to excute {__file__}")
    

    def test_td_34687(self):
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

        table_count = 100000 # 1000 is enough to reproduce the issue
        tag_length = 1026

        tdSql.prepare("db", True, vgroups=1)

        tdLog.info("create super table")
        tdSql.execute(f"create stable db.stb (ts timestamp, c1 int, c2 int) tags (tag1 varchar({tag_length}))")

        tdLog.info("create tables")
        for i in range(1, table_count):
            if i % 10000 == 0:
                tdLog.info(f"create table db.t{i}")
            tag = ''.join(random.choice(string.ascii_letters) for _ in range(tag_length))
            tdSql.execute(f"create table db.t{i} using db.stb tags ('{tag}')")

        tdLog.info("drop tables")
        for i in range(1, table_count):
            if i % 10000 == 0:
                tdLog.info(f"drop table db.t{i}")
            tdSql.execute(f"drop table db.t{i}")


        # Cleanup from original stop method
        tdLog.success(f"{__file__} successfully executed")


    