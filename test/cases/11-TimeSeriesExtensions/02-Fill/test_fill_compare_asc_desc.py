from new_test_framework.utils import tdLog, tdSql


class TestFillCompareAscDesc:

    def setup_class(cls):
        tdSql.prepare()
        tdSql.execute("use db;")
        # data for fill(prev)
        tdSql.execute("create stable st_pre (ts timestamp, c1 int) tags(t1 int);")
        tdSql.execute("create table ct1 using st_pre tags(1);")
        start_ts = 1705783972000
        sql = "insert into ct1 values "
        for i in range(100):
            sql += f"({start_ts + i * 1000}, {str(i+1)})"
        sql += ";"
        tdSql.execute(sql)
        
        # data for fill(next)
        tdSql.execute("create stable st_next (ts timestamp, c1 int) tags(t1 int);")
        tdSql.execute("create table ct2 using st_next tags(1);")
        start_ts = 1705783972000
        sql = "insert into ct1 values "
        for i in range(100):
            sql += f"({start_ts + i * 1000}, NULL)"
        sql += ";"
        tdSql.execute(sql)

        # data for fill(linear)
        tdSql.execute("create stable st_linear (ts timestamp, c1 int) tags(t1 int);")
        tdSql.execute("create table ct3 using st_linear tags(1);")
        start_ts = 1705783972000
        sql = "insert into ct1 values "
        for i in range(100):
            if i % 2 == 0:
                sql += f"({start_ts + i * 1000}, {str(i+1)})"
            else:
                sql += f"({start_ts + i * 1000}, NULL)"
        sql += ";"
        tdSql.execute(sql)
        tdLog.info("prepare data done")

    def test_fill_pre_compare_asc_desc(self):
        """test fill(prev) {func} compare asc and desc

        test select func(avg(c1)", "count(c1)", "first(c1)", "last(c1)", "max(c1)", "min(c1)", "sum(c1)) fill(prev) order by _wstart asc / desc

        Since: v3.3.0.0

        Labels: fill

        Jira:

        History:
            - 2024-8-13 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use db;")
        for func in ["avg(c1)", "count(c1)", "first(c1)", "last(c1)", "max(c1)", "min(c1)", "sum(c1)"]:
            tdSql.query(f"select _wstart, {func} from st_pre where ts between '2024-01-21 04:52:52.000' and '2024-01-21 04:54:31.000' interval(5s) fill(prev) order by _wstart asc;")
            res1 = tdSql.res
            tdSql.query(f"select _wstart, {func} from st_pre where ts between '2024-01-21 04:52:52.000' and '2024-01-21 04:54:31.000' interval(5s) fill(prev) order by _wstart desc;")
            res2 = tdSql.res
            assert len(res1) == len(res2)
            for i in range(len(res1)):
                assert res1[i] in res2
            tdLog.info(f"fill(prev) {func} compare asc and desc done")
        tdLog.info("Finish the test case 'test_fill_pre_compare_asc_desc'")

    def test_fill_next_compare_asc_desc(self):
        """test fill(next) {func} compare asc and desc

        test select func(avg(c1)", "count(c1)", "first(c1)", "last(c1)", "max(c1)", "min(c1)", "sum(c1)) fill(next) order by _wstart asc / desc

        Since: v3.3.0.0

        Labels: fill

        Jira:

        History:
            - 2024-8-13 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use db;")
        for func in ["avg(c1)", "count(c1)", "first(c1)", "last(c1)", "max(c1)", "min(c1)", "sum(c1)"]:
            tdSql.query(f"select _wstart, {func} from st_next where ts between '2024-01-21 04:52:52.000' and '2024-01-21 04:54:31.000' interval(5s) fill(next) order by _wstart asc;")
            res1 = tdSql.res
            tdSql.query(f"select _wstart, {func} from st_next where ts between '2024-01-21 04:52:52.000' and '2024-01-21 04:54:31.000' interval(5s) fill(next) order by _wstart desc;")
            res2 = tdSql.res
            assert len(res1) == len(res2)
            for i in range(len(res1)):
                assert res1[i] in res2
            tdLog.info(f"fill(next) {func} compare asc and desc done")
        tdLog.info("Finish the test case 'test_fill_next_compare_asc_desc'")

    def test_fill_linear_compare_asc_desc(self):
        """test fill(linear) {func} compare asc and desc

       test select func(avg(c1)", "count(c1)", "first(c1)", "last(c1)", "max(c1)", "min(c1)", "sum(c1)) fill(linear) order by _wstart asc / desc

       Since: v3.3.0.0

       Labels: fill

       Jira:

       History:
           - 2024-8-13 Feng Chao Created
           - 2025-5-08 Huo Hong Migrated to new test framework

       """
        tdSql.execute("use db;")
        for func in ["avg(c1)", "count(c1)", "first(c1)", "last(c1)", "max(c1)", "min(c1)", "sum(c1)"]:
            tdSql.query(f"select _wstart, {func} from st_linear where ts between '2024-01-21 04:52:52.000' and '2024-01-21 04:54:31.000' interval(5s) fill(linear) order by _wstart asc;")
            res1 = tdSql.res
            tdSql.query(f"select _wstart, {func} from st_linear where ts between '2024-01-21 04:52:52.000' and '2024-01-21 04:54:31.000' interval(5s) fill(linear) order by _wstart desc;")
            res2 = tdSql.res
            assert len(res1) == len(res2)
            for i in range(len(res1)):
                assert res1[i] in res2
            tdLog.info(f"fill(linear) {func} compare asc and desc done")
        tdLog.info("Finish the test case 'test_fill_linear_compare_asc_desc'")
