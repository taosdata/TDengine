from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestUnion:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ts6660(self):
        """Operator union order by

        1. Create 1 database 1 stable 2 subtables
        2. Insert data into 2 subtables 1 rows each
        3. Use union operator to combine the result from 2 subtables
        4. Use order by pseudo columns tbname, _wstart in union query
        5. Use number column in order by clause in union query
        6. Check error when order by column is not in select columns in union query
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TS-6660

        History:
            - 2025-8-11 Jinqing Kuang add test

        """

        db = "ts6660"
        stb = "stb"

        tdSql.execute(f"create database if not exists {db} keep 36500;")
        tdSql.execute(f"use {db};")

        tdLog.info("====== start create table and insert data")
        tdSql.execute(
            f"create table {stb} (attr_time timestamp, BAY_fwdwh_main double) tags (attr_id nchar(256));"
        )
        tdSql.execute(
            f"create table t1 using {stb} (attr_id) tags ('CN_53_N0007_S0001_BAY0001');"
        )
        tdSql.execute(
            f"create table t2 using {stb} (attr_id) tags ('CN_53_N0007_S0001_BAY0002');"
        )
        tdSql.execute(
            f"insert into t1 values ('2025-06-13 00:00:00.0', 130000), ('2025-06-13 07:30:00.0', 130730), ('2025-06-13 07:31:00.0', 130731);"
        )
        tdSql.execute(
            f"insert into t2 values ('2025-06-13 00:00:00.0', 130000), ('2025-06-13 07:30:00.0', 130730), ('2025-06-13 07:31:00.0', 130731);"
        )

        tdSql.error(
            f"""
                    select attr_id, attr_time, BAY_fwdwh_main num
                    from {stb}
                    where attr_time = '2025-06-13 00:00:00.0'
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    UNION
                    select attr_id, _wstart, first(BAY_fwdwh_main) num
                    from {stb}
                    where (attr_time = '2025-06-13 07:30:00.0' or attr_time = '2025-06-13 07:31:00.0')
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    partition by attr_id interval(1m) fill(null)
                    ORDER BY _wstart;"""
        )

        tdSql.query(
            f"""
                    select attr_id, attr_time, BAY_fwdwh_main num
                    from {stb}
                    where attr_time = '2025-06-13 00:00:00.0'
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    UNION
                    select attr_id, _wstart as attr_time, first(BAY_fwdwh_main) num
                    from {stb}
                    where (attr_time = '2025-06-13 07:30:00.0' or attr_time = '2025-06-13 07:31:00.0')
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    partition by attr_id interval(1m) fill(null)
                    ORDER BY attr_time;"""
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"""
                    select attr_id, attr_time, BAY_fwdwh_main num
                    from {stb}
                    where attr_time = '2025-06-13 00:00:00.0'
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    UNION
                    select attr_id, _wstart as attr_time, first(BAY_fwdwh_main) num
                    from {stb}
                    where (attr_time = '2025-06-13 07:30:00.0' or attr_time = '2025-06-13 07:31:00.0')
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    partition by attr_id interval(1m) fill(null)
                    ORDER BY 2;"""
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"""
                    select attr_id, attr_time, BAY_fwdwh_main num
                    from {stb}
                    where attr_time = '2025-06-13 00:00:00.0'
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    UNION
                    (select attr_id, _wstart, first(BAY_fwdwh_main) num
                    from {stb}
                    where (attr_time = '2025-06-13 07:30:00.0' or attr_time = '2025-06-13 07:31:00.0')
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    partition by attr_id interval(1m) fill(null)
                    ORDER BY _wstart);"""
        )
        tdSql.checkRows(6)

    def test_setOp_orderBy_pseudo(self):
        """Operator union

        1. Create 1 database 1 stable 3 subtables
        2. Insert data into 3 subtables 1 rows each
        3. Use union operator to combine the result from 3 subtables
        4. Use order by pseudo columns tbname, _wstart in union query
        

        Since: v3.0.0.0

        Labels: set operator, order by, pseudo function

        Jira: TS-7311

        History:
            - 2025-09-24 Tianyi Zhang created

        """
        db = "ts7311"
        tdSql.execute(f"create database if not exists {db} keep 36500;")
        tdSql.execute(f"use {db};")
        tdSql.execute(f"create table stb (ts timestamp, v1 int) tags (t1 int);")
        tdSql.execute(f"create table t1 using stb tags (1);")
        tdSql.execute(f"create table t2 using stb tags (2);")
        tdSql.execute(f"create table t3 using stb tags (3);")
        tdSql.execute(f"insert into t1 values ('2023-01-01 01:00:00', 1), ('2023-01-02 02:00:00', 2);")
        tdSql.execute(f"insert into t2 values ('2023-01-01 03:00:00', 3), ('2023-01-02 04:00:00', 4);")
        tdSql.execute(f"insert into t3 values ('2023-01-01 05:00:00', 5), ('2023-01-02 06:00:00', 6);")

        # order by tbname
        tdSql.query("(select tbname, ts, v1 from t1) union (select tbname, ts, v1 from t2) union (select tbname, ts, v1 from t3) order by tbname, ts desc;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(5, 2, 5)

        # error because select columns do not include tbname
        tdSql.error("(select ts, v1 from t1) \
                    union (select ts, v1 from t2) \
                    union (select ts, v1 from t3) \
                    order by tbname, ts desc;") 

        # order by _wstart
        tdSql.error("select * from t1 order by _wstart;")
        tdSql.error("(select tbname, ts, v1 from t1) union (select tbname, ts, v1 from t2) order by _wstart;")

        tdSql.query("(select _wstart, avg(v1) v1 from t1 count_window(2)) \
                    union (select _wstart, avg(v1) v1 from t2 count_window(2)) \
                    union (select _wstart, avg(v1) v1 from t3 count_window(2)) \
                    order by _wstart;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1.5)
        tdSql.checkData(1, 1, 3.5)
        tdSql.checkData(2, 1, 5.5)

        tdSql.query("(select _wstart ts, avg(v1) v1 from t1 count_window(2)) \
                    union (select _wstart ts, avg(v1) v1 from t2 count_window(2)) \
                    union (select _wstart ts, avg(v1) v1 from t3 count_window(2)) \
                    order by ts;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1.5)
        tdSql.checkData(1, 1, 3.5)
        tdSql.checkData(2, 1, 5.5)

        tdSql.noError("select _wstart, sum(v1) v1 from t1 interval(1s) \
                    union select ts, v1 from t2 \
                    order by _wstart;")
        tdSql.noError("select _wstart, sum(v1) v1 from t1 interval(1s) \
                    union select ts, v1 from t2 \
                    order by 1;") # use column index
        tdSql.noError("select _wstart ts, sum(v1) v1 from t1 interval(1s) \
                    union select ts, v1 from t2 \
                    order by ts;") # use alias

    def test_setOp_orderby_normal_func(self):
        """Operator union order by functions

        1. Create 1 database 1 stable 3 subtables
        2. Insert data into 3 subtables 1 rows each
        3. Use union operator to combine the result from 3 subtables
        4. Use normal functions abs(), ltrim(), lower() in order by clause in union query
        
        Since: v3.0.0.0

        Labels: set operator, order by, normal function

        Jira: TS-7311

        History:
            - 2025-09-26 Tianyi Zhang created

        """
        db = "ts7311"
        tdSql.execute(f"create database if not exists {db} keep 36500;")
        tdSql.execute(f"use {db};")
        tdSql.execute("create table tb1 (ts timestamp, vint int, vstr varchar(20))")
        tdSql.execute("create table tb2 (ts timestamp, vint int, vstr varchar(20))")
        tdSql.execute("create table tb3 (ts timestamp, vint int, vstr varchar(20))")
        tdSql.execute("insert into tb1 values "
                        "('2025-09-10 12:00:00', 1,  'Abc')" \
                        "('2025-09-10 13:00:00', 1,  ' bc')" \
                        "('2025-09-10 14:00:00', 3,  'Ccd')")
        tdSql.execute("insert into tb2 values "
                        "('2025-09-10 15:00:00', -5, ' aaa')" \
                        "('2025-09-10 16:00:00',  7, 'd')" \
                        "('2025-09-10 17:00:00',  4, 'Ccd')")
        tdSql.execute("insert into tb1 values "
                        "('2025-09-10 18:00:00',  1, 'cBA')" \
                        "('2025-09-10 19:00:00', -2, 'hhh')" \
                        "('2025-09-10 20:00:00', -9, 'ZZZ')")
        
        # 'sum(vint)' is invalid
        tdSql.error("(select ts, abs(vint), ltrim(vstr) from tb1) union (select ts, abs(vint), ltrim(vstr) from tb2) union (select ts, abs(vint), ltrim(vstr) from tb3) order by sum(vint), ltrim(vstr);")

        # 'vstr' is invalid
        tdSql.error("(select ts, abs(vint) vint, ltrim(vstr) from tb1) union (select ts, abs(vint) vint, ltrim(vstr) from tb2) union (select ts, abs(vint) vint, ltrim(vstr) from tb3) order by abs(vint), ltrim(vstr);")

        tdSql.query("(select ts, abs(vint) vint, vstr from tb1) union (select ts, abs(vint) vint, vstr from tb2) union (select ts, abs(vint) vint, vstr from tb3) order by abs(vint) desc, ts;")
        expected = [
            ("2025-09-10 20:00:00.000", 9, "ZZZ"),
            ("2025-09-10 16:00:00.000", 7, "d"),
            ("2025-09-10 15:00:00.000", 5, " aaa"),
            ("2025-09-10 17:00:00.000", 4, "Ccd"),
            ("2025-09-10 14:00:00.000", 3, "Ccd"),
            ("2025-09-10 19:00:00.000", 2, "hhh"),
            ("2025-09-10 12:00:00.000", 1, "Abc"),
            ("2025-09-10 13:00:00.000", 1, " bc"),
            ("2025-09-10 18:00:00.000", 1, "cBA"),
        ]
        tdSql.checkRows(len(expected))
        for row_idx, row_data in enumerate(expected):
            for col_idx, cell_data in enumerate(row_data):
                tdSql.checkData(row_idx, col_idx, cell_data)

        # 'vint' is invalid
        tdSql.error("(select ts, abs(vint), vstr from tb1) union (select ts, abs(vint), vstr from tb2) union (select ts, abs(vint), vstr from tb3) order by ltrim(lower(vstr)), abs(vint);")
        tdSql.query("(select ts, abs(vint), vstr from tb1) union (select ts, abs(vint), vstr from tb2) union (select ts, abs(vint), vstr from tb3) order by ltrim(lower(vstr)), ts;")
        expected = [
            ("2025-09-10 15:00:00.000", 5, " aaa"),
            ("2025-09-10 12:00:00.000", 1, "Abc"),
            ("2025-09-10 13:00:00.000", 1, " bc"),
            ("2025-09-10 18:00:00.000", 1, "cBA"),
            ("2025-09-10 14:00:00.000", 3, "Ccd"),
            ("2025-09-10 17:00:00.000", 4, "Ccd"),
            ("2025-09-10 16:00:00.000", 7, "d"),
            ("2025-09-10 19:00:00.000", 2, "hhh"),
            ("2025-09-10 20:00:00.000", 9, "ZZZ"),
        ]
        tdSql.checkRows(len(expected))
        for row_idx, row_data in enumerate(expected):
            for col_idx, cell_data in enumerate(row_data):
                tdSql.checkData(row_idx, col_idx, cell_data)

        tdSql.query("(select ts, abs(vint) vint, ltrim(vstr) vstr from tb1) union (select ts, abs(vint) vint, ltrim(vstr) vstr from tb2) union (select ts, abs(vint) vint, ltrim(vstr) vstr from tb3) order by vint, ltrim(vstr);")
        expected = [
            ("2025-09-10 12:00:00.000", 1, "Abc"),
            ("2025-09-10 13:00:00.000", 1, "bc"),
            ("2025-09-10 18:00:00.000", 1, "cBA"),
            ("2025-09-10 19:00:00.000", 2, "hhh"),
            ("2025-09-10 14:00:00.000", 3, "Ccd"),
            ("2025-09-10 17:00:00.000", 4, "Ccd"),
            ("2025-09-10 15:00:00.000", 5, "aaa"),
            ("2025-09-10 16:00:00.000", 7, "d"),
            ("2025-09-10 20:00:00.000", 9, "ZZZ"),
        ]
        tdSql.checkRows(len(expected))
        for row_idx, row_data in enumerate(expected):
            for col_idx, cell_data in enumerate(row_data):
                tdSql.checkData(row_idx, col_idx, cell_data)

    def test_setOp_with_const_condition(self):
        """union with const condition

        test for 'where 1=0' condition in UNION

        Catalog:
            - Query:Union

        Since: v3.8.0.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TD-38544

        History:
            - 2025-11-12 Jing Sima add test

        """
        db = "td38544"
        tdSql.execute(f"create database if not exists {db} keep 36500;")
        tdSql.execute(f"use {db};")
        tdSql.execute("create stable s1(ts timestamp, deviceid binary(16), c0 int, c1 int) tags(tg binary(8))")
        tdSql.execute("create table t1 using s1 tags('X')")
        tdSql.execute("insert into t1 values('2025-11-12 14:03:38.293', 'devA', 119,7)")
        tdSql.execute("insert into t1 values('2025-11-13 14:03:38.293', 'devA', 119,8)")
        tdSql.execute("insert into t1 values('2025-11-14 14:03:38.293', 'devA', 119,9)")
        tdSql.query("(select ts, deviceid, c0,c1 from t1 where c0 =119) union all (select ts, deviceid, c0,c1 from t1 where 1=0)")
        tdSql.checkRows(3)
        tdSql.query("(select ts, deviceid, c0,c1 from t1 where 1=0) union all (select ts, deviceid, c0,c1 from t1 where 1=0)")
        tdSql.checkRows(0)
        tdSql.query("(select ts, deviceid, c0,c1 from t1 where c0=119) union all (select ts, deviceid, c0,c1 from t1 where c0=119)")
        tdSql.checkRows(6)