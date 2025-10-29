from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFuncAggSmoking:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_agg_smoking(self):
        """Agg-basic: smoking cases

        Smoking the aggregate functions

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-22 Stephen Jin

        """

        tdStream.dropAllStreamsAndDbs()
        self.smoking_std_variance()
        tdStream.dropAllStreamsAndDbs()
        #self.QueryStddev()
        #tdStream.dropAllStreamsAndDbs()

    def smoking_gconcat(self):
        db = "testdb"
        tb = "testtb"
        rowNum = 10

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol varchar(10))")

        x = 0
        while x < rowNum:
            ms = 1601481600000 + x * 60000
            xfield = str(x)
            tdSql.execute(f"insert into {tb} values ({ms} , {xfield})")

            x = x + 1

        tdSql.query(f"select group_concat(tbcol, '?') from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '0?1?2?3?4?5?6?7?8?9')

    def smoking_std_variance(self):
        db = "testdb"
        tb = "testtb"
        rowNum = 5

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol int)")

        x = 0
        while x < rowNum:
            ms = 1601481600000 + x * 60000
            tdSql.execute(f"insert into {tb} values ({ms} , {x + 1})")

            x = x + 1

        tdSql.query(f"select std(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1.4142135623730951')

        tdSql.query(f"select variance(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2.0')

        tdSql.query(f"select stddev_samp(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1.5811388300841898')

        tdSql.query(f"select var_samp(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2.5')


    def test_fun_agg_stddev_samp(self):
        """ Fun: stddev_samp()

        1. create normal table with timestamp,int columns
        2. insert 5 rows with int values 1,2,3,4,5
        3. query stddev_samp(int column) and check the result

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-10-16 Alex Duan add doc

        """
        pass

    def test_fun_agg_variance(self):
        """ Fun: variance()

        1. create normal table with timestamp,int columns
        2. insert 5 rows with int values 1,2,3,4,5
        3. query variance(int column) and check the result

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-10-16 Alex Duan add doc

        """
        pass

    def test_func_agg_var_pop(self):
        """ Fun: var_pop()

        same with variance()

        Since: v3.0.0.0

        Labels: common,ci

        """
        pass

    def test_fun_agg_var_samp(self):
        """ Fun: var_samp()

        1. create normal table with timestamp,int columns
        2. insert 5 rows with int values 1,2,3,4,5
        3. query var_samp(int column) and check the result

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-10-16 Alex Duan add doc

        """
        pass

    def test_fun_agg_group_concat(self):
        """ Fun: group_concat()

        1. create normal table with timestamp,int columns
        2. insert 10 rows with int values 1,2,3,4,5,6,7,8,9,10
        3. query group_concat(int column) and check the result

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-10-16 Alex Duan add doc

        """
        self.smoking_gconcat()