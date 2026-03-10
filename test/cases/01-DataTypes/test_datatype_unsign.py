from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeUnsigned:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_unsigned(self):
        """DataTypes: unsigned numeric

        1. Create table
        2. Insert data
        3. Alter tag value
        4. Handle illegal input

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_unsign.sim

        """

        tdSql.execute(
            f"create table mt_unsigned (ts timestamp, a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned, e tinyint, f smallint, g int, h bigint, j bool) tags(t1 tinyint unsigned, t2 smallint unsigned, t3 int unsigned, t4 bigint unsigned, t5 tinyint, t6 smallint, t7 int, t8 bigint);"
        )
        tdSql.execute(
            f"create table mt_unsigned_1 using mt_unsigned tags(0, 0, 0, 0, 0, 0, 0, 0);"
        )

        tdSql.execute(f"alter table mt_unsigned_1 set tag t1=138;")
        tdSql.execute(f"alter table mt_unsigned_1 set tag t2=32769;")
        tdSql.execute(f"alter table mt_unsigned_1 set tag t3=294967295;")
        tdSql.execute(f"alter table mt_unsigned_1 set tag t4=446744073709551615;")
        tdSql.execute(
            f"insert into mt_unsigned_1 values(now, 0, 0, 0, 0, 0, 0, 0, 0, 0)"
        )

        tdSql.query(f"select t1,t2,t3,t4 from mt_unsigned_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 32769)
        tdSql.checkData(0, 2, 294967295)
        tdSql.checkData(0, 3, 446744073709551615)

        tdSql.error(f"sql alter table mt_unsigned_1 set tag t1 = 999;")
        tdSql.error(f"sql alter table mt_unsigned_1 set tag t2 = 95535;")
        tdSql.error(f"sql alter table mt_unsigned_1 set tag t3 = 8294967295l;")
        tdSql.error(f"sql alter table mt_unsigned_1 set tag t4 = 19446744073709551615;")

        tdSql.error(
            f"create table mt_unsigned_2 using mt_unsigned tags(-1, 0, 0, 0, 0, 0, 0, 0);"
        )
        tdSql.error(
            f"create table mt_unsigned_3 using mt_unsigned tags(0, -1, 0, 0, 0, 0, 0, 0);"
        )
        tdSql.error(
            f"create table mt_unsigned_4 using mt_unsigned tags(0, 0, -1, 0, 0, 0, 0, 0);"
        )
        tdSql.error(
            f"create table mt_unsigned_5 using mt_unsigned tags(0, 0, 0, -1, 0, 0, 0, 0);"
        )

        tdSql.execute(
            f"create table mt_unsigned_21 using mt_unsigned tags(255, 0, 0, 0, 0, 0, 0, 0);"
        )
        tdSql.execute(
            f"create table mt_unsigned_31 using mt_unsigned tags(0, 65535, 0, 0, 0, 0, 0, 0);"
        )
        tdSql.execute(
            f"create table mt_unsigned_41 using mt_unsigned tags(0, 0, 4294967295, 0, 0, 0, 0, 0);"
        )
        tdSql.execute(
            f"create table mt_unsigned_51 using mt_unsigned tags(0, 0, 0, 18446744073709551615, 0, 0, 0, 0);"
        )

        tdSql.error(
            f"create table mt_unsigned_2 using mt_unsigned tags(999, 0, 0, 0, 0, 0, 0, 0);"
        )
        tdSql.error(
            f"create table mt_unsigned_3 using mt_unsigned tags(0, 95535, 0, 0, 0, 0, 0, 0);"
        )
        tdSql.error(
            f"create table mt_unsigned_4 using mt_unsigned tags(0, 0, 5294967295l, 0, 0, 0, 0, 0);"
        )
        tdSql.error(
            f"create table mt_unsigned_5 using mt_unsigned tags(0, 0, 0, 28446744073709551615u, 0, 0, 0, 0);"
        )

        tdSql.execute(f"alter table mt_unsigned_1 set tag t1=NULL;")
        tdSql.execute(f"alter table mt_unsigned_1 set tag t2=NULL;")
        tdSql.execute(f"alter table mt_unsigned_1 set tag t3=NULL;")
        tdSql.execute(f"alter table mt_unsigned_1 set tag t4=NULL;")
        tdSql.query(f"select t1,t2,t3,t4 from mt_unsigned_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)

        tdSql.execute(
            f"insert into mt_unsigned_1 values(now+1s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
        )
        tdSql.execute(
            f"insert into mt_unsigned_1 values(now+2s, 1, 2, 3, 4, 5, 6, 7, 8, 9);"
        )
        tdSql.error(
            f"insert into mt_unsigned_1 values(now+3s, -1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
        )
        tdSql.error(
            f"insert into mt_unsigned_1 values(now+4s, NULL, -1, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
        )
        tdSql.error(
            f"insert into mt_unsigned_1 values(now+5s, NULL, NULL, -1, NULL, NULL, NULL, NULL, NULL, NULL);"
        )
        tdSql.error(
            f"insert into mt_unsigned_1 values(now+6s, NULL, NULL, NULL, -1, NULL, NULL, NULL, NULL, NULL);"
        )
        tdSql.execute(
            f"insert into mt_unsigned_1 values(now+7s, 255, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
        )
        tdSql.execute(
            f"insert into mt_unsigned_1 values(now+8s, NULL, 65535, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
        )
        tdSql.execute(
            f"insert into mt_unsigned_1 values(now+9s, NULL, NULL, 4294967295, NULL, NULL, NULL, NULL, NULL, NULL);"
        )
        tdSql.execute(
            f"insert into mt_unsigned_1 values(now+10s, NULL, NULL, NULL, 18446744073709551615, NULL, NULL, NULL, NULL, NULL);"
        )

        tdSql.query(
            f"select count(a),count(b),count(c),count(d), count(e) from mt_unsigned_1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select a+b+c from mt_unsigned_1 where a is null;")
        tdSql.checkRows(4)

        tdSql.query(f"select count(*), a from mt_unsigned_1 group by a;")
        tdSql.checkRows(4)

        tdSql.query(f"select count(*), b from mt_unsigned_1 group by b;")
        tdSql.checkRows(4)

        tdSql.query(f"select count(*), c from mt_unsigned_1 group by c;")
        tdSql.checkRows(4)

        tdSql.query(f"select count(*), d from mt_unsigned_1 group by d;")
        tdSql.checkRows(4)

        ## todo insert more rows and chec it
        tdSql.query(
            f"select first(a),count(b),last(c),sum(b),spread(d),avg(c),min(b),max(a),stddev(a) from mt_unsigned_1;"
        )
        tdSql.checkRows(1)
