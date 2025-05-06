from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagDropTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_drop_table(self):
        """删除子表后的标签查询

        1. 创建超级表
        2. 创建子表并写入数据
        3. 删除部分子表，确认生效
        4. 使用标签进行查询，确保结果中不包含被删除的子表

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/drop_tag.sim

        """

        dbPrefix = "ta_bib_db"
        tbPrefix = "ta_bib_tb"
        mtPrefix = "ta_bib_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"======== test bigint")
        tdSql.execute(
            f"create table if not exists st( ts timestamp, order_id bigint) tags (account_id bigint)"
        )

        tdSql.execute(f"create table t1 using st tags(111)")
        tdSql.execute(f"create table t2 using st tags(222)")

        tdSql.execute(f"insert into t1(ts, order_id) values(1648791213001, 1)")
        tdSql.execute(f"insert into t2(ts, order_id) values(1648791213002, 2)")
        tdSql.query(
            f"select  account_id,count(*) from st where account_id = 111  group by account_id"
        )

        tdSql.execute(f"drop table t1")

        tdSql.execute(f"create table t1 using st tags(111)")

        tdSql.execute(f"insert into t1(ts, order_id) values(1648791213001, 1)")
        tdSql.execute(f"insert into t2(ts, order_id) values(1648791213002, 2)")

        tdSql.query(
            f"select account_id,count(*) from st where account_id = 111  group by account_id"
        )

        tdSql.checkRows(1)

        tdLog.info(f"======== test varchar")

        tdSql.execute(f"drop stable st")

        tdSql.execute(
            f"create table if not exists st( ts timestamp, order_id bigint) tags (account_id binary(16))"
        )

        tdSql.execute(f'create table t1 using st tags("aac")')
        tdSql.execute(f'create table t2 using st tags("abc")')

        tdSql.execute(f"insert into t1(ts, order_id) values(1648791213001, 1)")
        tdSql.execute(f"insert into t2(ts, order_id) values(1648791213002, 2)")
        tdSql.query(
            f'select  account_id,count(*) from st where account_id = "aac"  group by account_id'
        )

        tdSql.execute(f"drop table t1")

        tdSql.execute(f'create table t1 using st tags("aac")')

        tdSql.execute(f"insert into t1(ts, order_id) values(1648791213001, 1)")
        tdSql.execute(f"insert into t2(ts, order_id) values(1648791213002, 2)")

        tdSql.query(
            f'select account_id,count(*) from st where account_id = "aac"  group by account_id'
        )

        tdSql.checkRows(1)

        tdLog.info(f"====== test empty table")
        tdSql.execute(f"drop table t1")

        tdSql.query(
            f'select account_id,count(*) from st where account_id = "aac"  group by account_id'
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
