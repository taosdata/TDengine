import time

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCompositeKeyLoad:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_project_column_1(self):
        """读取内存中数据和 stt 中复合主键数据出现错误

        原因是：minKey 采用浅拷贝，导致stt向后迭代时候，修改了 minKey 的值， 导致内存中的 key 和 stt 中key 合并后， 出现错误

        1. 创建单表，时间列和composite key 字符串列。向 stt 中写入 4 条记录，内存中写入一条记录
        2. 查询出错

        Catalog:
            - Query:SelectList

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6787

        History:
            - 2025-07-04 Haojun Liao

        """

        dbPrefix = "m_si_db"

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table t(ts timestamp, k varchar(12) primary key)")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00001')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00003')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00005')")
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00007')")
        tdSql.execute(f"flush database {db}; ")

        time.sleep(5)
        tdSql.execute(f"insert into t values('2025-01-01 1:1:1', 'abcd-00005')")

        tdLog.info("insert completed, check results")
        tdSql.query("select * from t")

        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 'abcd-00001')
        tdSql.checkData(1, 1, 'abcd-00003')
        tdSql.checkData(2, 1, 'abcd-00005')
        tdSql.checkData(3, 1, 'abcd-00007')

        print(f"=============== {tdSql.getData(1,1)}")

