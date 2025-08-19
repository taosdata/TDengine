from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUnionAllAsTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_unionall_as_table(self):
        """union all as table

        1. -

        Catalog:
            - Query:Union

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/unionall_as_table.sim

        """

        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"CREATE STABLE bw_yc_h_substation_mea (ts TIMESTAMP, create_date VARCHAR(50), create_time VARCHAR(30), load_time TIMESTAMP, sum_p_value FLOAT, sum_sz_value FLOAT, sum_gl_ys FLOAT, sum_g_value FLOAT) TAGS (id VARCHAR(50), name NCHAR(200), datasource VARCHAR(50), sys_flag VARCHAR(50));"
        )
        tdSql.execute(
            f"CREATE STABLE aw_yc_h_substation_mea (ts TIMESTAMP, create_date VARCHAR(50), create_time VARCHAR(30), load_time TIMESTAMP, sum_p_value FLOAT, sum_sz_value FLOAT, sum_gl_ys FLOAT, sum_g_value FLOAT) TAGS (id VARCHAR(50), name NCHAR(200), datasource VARCHAR(50), sys_flag VARCHAR(50));"
        )
        tdSql.execute(
            f"CREATE STABLE dw_yc_h_substation_mea (ts TIMESTAMP, create_date VARCHAR(50), create_time VARCHAR(30), load_time TIMESTAMP, sum_p_value FLOAT, sum_sz_value FLOAT, sum_gl_ys FLOAT, sum_g_value FLOAT) TAGS (id VARCHAR(50), name NCHAR(200), datasource VARCHAR(50), sys_flag VARCHAR(50));"
        )
        tdSql.execute(
            f"insert into t1 using dw_yc_h_substation_mea tags('1234567890','testa','0021001','abc01') values(now,'2023-03-27','00:01:00',now,2.3,3.3,4.4,5.5);"
        )
        tdSql.execute(
            f"insert into t2 using dw_yc_h_substation_mea tags('2234567890','testb','0022001','abc02') values(now,'2023-03-27','00:01:00',now,2.3,2.3,2.4,2.5);"
        )
        tdSql.execute(
            f"insert into t3 using aw_yc_h_substation_mea tags('2234567890','testc','0023001','abc03') values(now,'2023-03-27','00:15:00',now,2.3,2.3,2.4,2.5);"
        )
        tdSql.execute(
            f"insert into t4 using bw_yc_h_substation_mea tags('4234567890','testd','0021001','abc03') values(now,'2023-03-27','00:45:00',now,2.3,2.3,2.4,2.5);"
        )
        tdSql.execute(
            f"insert into t5 using bw_yc_h_substation_mea tags('5234567890','testd','0021001','abc03') values(now,'2023-03-27','00:00:00',now,2.3,2.3,2.4,2.5);"
        )
        tdSql.query(
            f"select t.ts,t.id,t.name,t.create_date,t.create_time,t.datasource,t.sum_p_value from (select ts,id,name,create_date,create_time,datasource,sum_p_value from bw_yc_h_substation_mea where create_date='2023-03-27' and substr(create_time,4,2) in ('00','15','30','45') union all select ts,id,name,create_date,create_time,datasource,sum_p_value from aw_yc_h_substation_mea where create_date='2023-03-27' and substr(create_time,4,2) in ('00','15','30','45') union all select ts,id,name,create_date,create_time,datasource,sum_p_value from dw_yc_h_substation_mea where create_date='2023-03-27' and substr(create_time,4,2) in ('00','15','30','45'))  t where t.datasource='0021001' and t.id='4234567890' order by t.create_time;"
        )

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, "4234567890")

        tdSql.checkData(0, 5, "0021001")

        tdSql.execute(f"create table st (ts timestamp, f int) tags (t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values(now, 1)(now+1s, 2)")
        tdSql.execute(f"insert into ct2 using st tags(2) values(now+2s, 3)(now+3s, 4)")
        tdSql.query(
            f"select count(*) from (select * from ct1 union all select * from ct2)"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select count(*) from (select * from ct1 union select * from ct2)")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 4)

        tdSql.execute(f"create table ctcount(ts timestamp, f int);")
        tdSql.execute(f"insert into ctcount(ts) values(now)(now+1s);")
        tdSql.query(f"select count(*) from (select f from ctcount);")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(*) from (select f, f from ctcount)")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(*) from (select last(ts), first(ts) from ctcount);")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.error(f"select f from (select f, f from ctcount);")
