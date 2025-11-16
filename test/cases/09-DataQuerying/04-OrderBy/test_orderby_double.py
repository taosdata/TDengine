from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestOrderByDouble:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_orderby_double(self):
        """Order by double

        1. Create a database and table
        2. Insert double values into the table
        3. Query the table with order by double values; without the fix for TS-6772, it should be failed
        4. Verify the order of the returned results

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6772

        History:
            - 2025-7-5 Ethan liu add this test case for TS-6772 when order by double value

        """

        tdLog.info(f"=============== Start test Order By Double")

        # Data to be inserted into the table
        insertData = [[
            -0.000030000001000, -0.000001464545600, -0.000004819474300,
            -0.000032116670000, -0.000031666670000, -0.000003312820600,
            -0.000000384615420, -0.000045286994000, -0.000013181819000,
            -0.000025755055000, -0.000026000002000, -0.000009981204000,
            -0.000040256410000, -0.000009677977000, -0.000021196980000,
            -0.000044866140000, -0.000018861480000, -0.000017505552000,
            -0.000005940525000, -0.000040000003000, -0.000020000001000,
            -0.000023275863000, -0.000007150000300, -0.000006666666600,
            -0.000007098765800
        ],
        [
            -0.000000666666660,-0.000008333334000,-0.000023669540000,
            -0.000000033333336,-0.000014882526000,-0.000016531532000,
            -0.000011734235000,-0.000013855327000,-0.000000500000060,
            -0.000003293918800,0.000000002163109,-0.000017172620000,
            -0.000018333334000,-0.000011869206000,-0.000002192971400,
            -0.000006178862300,-0.000017367150000,-0.000021142021000,
            -0.000007368095300,-0.000014882526000,-0.000023275863000,
            -0.000011351892000,-0.000020134885000,-0.000002999999900,
            -0.000017595860000
        ],
        [
            -0.000016987470000,-0.000010000001000,-0.000030000001000,
            -0.000014347055000,-0.000009826597000,-0.000009677977000,
            0.000000440188070,-0.000005235746000,-0.000012462896000,
            -0.000032516310000,-0.000008435134000,-0.000011255557000,
            -0.000000065616800,-0.000004615710000,-0.000047639016000,
            -0.000046974900000,-0.000040709350000,-0.000011889764000,
            -0.000047832060000,0.000000370370400,-0.000000284429430,
            -0.000017500000000,-0.000028125001000,-0.000018274892000,
            -0.000028625169000
        ]]

        # ceate database and table
        tdSql.execute(f"drop database if exists test_order_by_double")
        tdSql.execute(f"create database test_order_by_double")
        tdSql.execute(f"use test_order_by_double")

        tdSql.execute(f"create table st(ts timestamp, flag double) tags(device int)")
        tdSql.execute(f"create table t0 using st tags (1)")
        tdSql.execute(f"create table t1 using st tags (2)")
        tdSql.execute(f"create table t2 using st tags (3)")

        # insert data into table
        for i in range(len(insertData)):
            for j in range(len(insertData[i])):
                value = insertData[i][j]
                # Insert data with a timestamp offset to ensure unique timestamps
                # and to avoid conflicts with the same timestamp in different rows.
                tdSql.execute(f"insert into t{i} values(now+{j}s, {value})")

        # query with order by
        tdSql.query(f"select * from t0")
        tdSql.checkRows(25)
        tdSql.query(f"select * from t0 order by flag")
        tdSql.checkRows(25)
        tdSql.checkData(0,1, -0.000045286994000)
        tdSql.checkData(1,1, -0.000044866140000)
        tdSql.checkData(2,1, -0.000040256410000)
        tdSql.checkData(3,1, -0.000040000003000)
        tdSql.checkData(4,1, -0.000032116670000)
        tdSql.checkData(5,1, -0.000031666670000)
        tdSql.checkData(6,1, -0.000030000001000)
        tdSql.checkData(7,1, -0.000026000002000)
        tdSql.checkData(8,1, -0.000025755055000)
        tdSql.checkData(9,1, -0.000023275863000)
        tdSql.checkData(10,1, -0.000021196980000)
        tdSql.checkData(11,1, -0.000020000001000)
        tdSql.checkData(12,1, -0.000018861480000)
        tdSql.checkData(13,1, -0.000017505552000)
        tdSql.checkData(14,1, -0.000013181819000)
        tdSql.checkData(15,1, -0.000009981204000)
        tdSql.checkData(16,1, -0.000009677977000)
        tdSql.checkData(17,1, -0.000007150000300)
        tdSql.checkData(18,1, -0.000007098765800)
        tdSql.checkData(19,1, -0.000006666666600)
        tdSql.checkData(20,1, -0.000005940525000)
        tdSql.checkData(21,1, -0.000004819474300)
        tdSql.checkData(22,1, -0.000003312820600)
        tdSql.checkData(23,1, -0.000001464545600)
        tdSql.checkData(24,1, -0.000000384615420)

        tdSql.query(f"select * from t1")
        tdSql.checkRows(25)
        tdSql.query(f"select * from t1 order by flag")
        tdSql.checkRows(25)
        tdSql.checkData(0,1, -0.000023669540000)
        tdSql.checkData(1,1, -0.000023275863000)
        tdSql.checkData(2,1, -0.000021142021000)
        tdSql.checkData(3,1, -0.000020134885000)
        tdSql.checkData(4,1, -0.000018333334000)
        tdSql.checkData(5,1, -0.000017595860000)
        tdSql.checkData(6,1, -0.000017367150000)
        tdSql.checkData(7,1, -0.000017172620000)
        tdSql.checkData(8,1, -0.000016531532000)
        tdSql.checkData(9,1, -0.000014882526000)
        tdSql.checkData(10,1, -0.000014882526000)
        tdSql.checkData(11,1, -0.000013855327000)
        tdSql.checkData(12,1, -0.000011869206000)
        tdSql.checkData(13,1, -0.000011734235000)
        tdSql.checkData(14,1, -0.000011351892000)
        tdSql.checkData(15,1, -0.000008333334000)
        tdSql.checkData(16,1, -0.000007368095300)
        tdSql.checkData(17,1, -0.000006178862300)
        tdSql.checkData(18,1, -0.000003293918800)
        tdSql.checkData(19,1, -0.000002999999900)
        tdSql.checkData(20,1, -0.000002192971400)
        tdSql.checkData(21,1, -0.000000666666660)
        tdSql.checkData(22,1, -0.000000500000060)
        tdSql.checkData(23,1, -0.000000033333336)
        tdSql.checkData(24,1, 0.000000002163109)

        tdSql.query(f"select * from t2")
        tdSql.checkRows(25)
        tdSql.query(f"select * from t2 order by flag")
        tdSql.checkRows(25)
        tdSql.checkData(0,1, -0.000047832060000)
        tdSql.checkData(1,1, -0.000047639016000)
        tdSql.checkData(2,1, -0.000046974900000)
        tdSql.checkData(3,1, -0.000040709350000)
        tdSql.checkData(4,1, -0.000032516310000)
        tdSql.checkData(5,1, -0.000030000001000)
        tdSql.checkData(6,1, -0.000028625169000)
        tdSql.checkData(7,1, -0.000028125001000)
        tdSql.checkData(8,1, -0.000018274892000)
        tdSql.checkData(9,1, -0.000017500000000)
        tdSql.checkData(10,1, -0.000016987470000)
        tdSql.checkData(11,1, -0.000014347055000)
        tdSql.checkData(12,1, -0.000012462896000)
        tdSql.checkData(13,1, -0.000011889764000)
        tdSql.checkData(14,1, -0.000011255557000)
        tdSql.checkData(15,1, -0.000010000001000)
        tdSql.checkData(16,1, -0.000009826597000)
        tdSql.checkData(17,1, -0.000009677977000)
        tdSql.checkData(18,1, -0.000008435134000)
        tdSql.checkData(19,1, -0.000005235746000)
        tdSql.checkData(20,1, -0.000004615710000)
        tdSql.checkData(21,1, -0.000000284429430)
        tdSql.checkData(22,1, -0.000000065616800)
        tdSql.checkData(23,1, 0.000000370370400)
        tdSql.checkData(24,1, 0.000000440188070)

        tdLog.info(f"=============== End test Order By Double")