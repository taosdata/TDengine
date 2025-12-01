from new_test_framework.utils import tdLog, tdSql

class TestFuncLastAsParam:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_last_as_operator_param(self):
        """Select: keepColumnName
        
        Test that 'last(col) - first(col)' is not equal to zero when keepColumnName is 1.

        Steps:
        1. Create a database and a stable table.
        2. Create sub-tables and insert data where the first and last values are different.
        3. Set 'alter local 'keepColumnName' '1''.
        4. Execute 'select last(tbcol) - first(tbcol) from stable_table group by tgcol'.
        5. Verify that the result is the difference between the last and first value, which is not zero.

        Since: 3.3.6.24

        Jira: TS-7262

        History:
            - 2025-09-11 Tony Zhang created
        """

        db = "db"
        stb = "stb"
        tbNum = 10
        rowNum = 20

        tdLog.info("=============== step1: prepare data ===============")
        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {stb} (ts timestamp, tbcol int) TAGS(tgcol int)")

        for i in range(tbNum):
            tb = f"ctb_{i}"
            tdSql.execute(f"create table {tb} using {stb} tags({i})")
            for x in range(rowNum):
                ms = 1601481600000 + (x * 60000)
                if x == 0 or x == rowNum - 1:
                    tdSql.execute(f"insert into {tb} values ({ms}, null)")
                else:
                    tdSql.execute(f"insert into {tb} values ({ms}, {x})")


        tdLog.info("=============== step2: set keepColumnName and query ===============")
        tdSql.execute("alter local 'keepColumnName' '1'")
        tdSql.query("show local variables like 'keepColumnName'")
        tdSql.checkData(0, 1, '1')

        
        tdLog.info("=============== step3: check results ===============")
        tdSql.query(f"select last(tbcol) - first(tbcol) from {stb} group by tgcol")
        # last(tbcol) is 18, first(tbcol) is 1. So 18 - 1 = 17.
        expected_diff = 17
        for i in range(tbNum):
            tdSql.checkData(i, 0, expected_diff)
        tdSql.checkRows(tbNum)


        tdLog.info("=============== step4: alter cachemodel to both and test again ===============")
        tdSql.execute(f"alter database {db} cachemodel 'both'")
        tdSql.query(f"select last(tbcol) - first(tbcol) from {stb} group by tgcol")
        expected_diff = 17
        for i in range(tbNum):
            tdSql.checkData(i, 0, expected_diff)
        tdSql.checkRows(tbNum)

        tdLog.info("=============== clear ===============")
        tdSql.execute(f"drop database {db}")

    def test_last_as_func_param(self):
        """Select: keepColumnName first/last

        Steps:
        1. Create a database and a stable table.
        2. Create sub-tables and insert data where the first and last timestamps are different.
        3. Set 'alter local 'keepColumnName' '1''.
        4. Execute 'select timediff(last(ts), first(ts)) from stable_table group by tgcol'.
        5. Verify that the result is the difference between the last and first timestamp, which is not zero.

        Since: 3.3.6.24

        Jira: https://jira.taosdata.com:18080/browse/TS-7262

        History:
            - 2025-09-11 Tony Zhang created
        """

        db = "db"
        stb = "stb"
        tbNum = 10
        rowNum = 20

        tdLog.info("=============== step1: prepare data ===============")
        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {stb} (ts timestamp, tbcol int) TAGS(tgcol int)")

        for i in range(tbNum):
            tb = f"ctb_{i}"
            tdSql.execute(f"create table {tb} using {stb} tags({i})")
            for x in range(rowNum):
                ms = 1601481600000 + (x * 60000)
                if x == 0 or x == rowNum - 1:
                    tdSql.execute(f"insert into {tb} values ({ms}, null)")
                else:
                    tdSql.execute(f"insert into {tb} values ({ms}, {x})")


        tdLog.info("=============== step2: set keepColumnName and query ===============")
        tdSql.execute("alter local 'keepColumnName' '1'")
        tdSql.query("show local variables like 'keepColumnName'")
        tdSql.checkData(0, 1, '1')

        
        tdLog.info("=============== step3: check results ===============")
        tdSql.query(f"select timediff(last(ts), first(ts)) from {stb} group by tgcol")
        # last(ts) - first(ts) = (rowNum - 1) * 60000 milliseconds
        expected_diff = (rowNum - 1) * 60000
        for i in range(tbNum):
            tdSql.checkData(i, 0, expected_diff)
        tdSql.checkRows(tbNum)
