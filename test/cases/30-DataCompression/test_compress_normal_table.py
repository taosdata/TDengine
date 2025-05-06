from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCompressNormalTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_compress_normal_table(self):
        """禁用压缩

        1. 创建一个包含 bool、tinyint、smallint、int、bigint、float、double、binary 数据类型的普通表
        2. 写入 2000 条记录
        3. 再创建另外两个数据库
        4. 分别写入不同的 2000 条记录
        5. kill 停止 taosd
        6. 检查已写入数据的条数


        Catalog:
            - Compress

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compress/compress.sim

        """

        tdLog.info(f"============================ dnode1 start")
        i = 0
        dbPrefix = "db"
        tbPrefix = "tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        N = 2000

        tdLog.info(f"=============== step1")
        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {tb} (ts timestamp, b bool encode 'disabled', t tinyint encode 'disabled', s smallint encode 'disabled', i int encode 'disabled', big bigint encode 'disabled', str binary(256))"
        )

        count = 0
        while count < N:
            ms = 1591200000000 + count
            tdSql.execute(
                f"insert into {tb} values(  {ms} , 1, 0, {count} , {count} , {count} ,'it is a string')"
            )
            count = count + 1

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(N)

        tdSql.execute(f"flush database {db}")

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(N)

        tdSql.execute(f"alter table {tb} modify column ts encode 'disabled'")

        count = 0
        while count < N:
            ms = 1591200030000 + count
            tdSql.execute(
                f"insert into {tb} values(  {ms} , 1, 0, {count} , {count} , {count} ,'it is a string')"
            )
            count = count + 1

        M = 4000
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(M)

        tdSql.execute(f"flush database {db}")

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(M)

        stb1 = "txx1"
        tdSql.execute(
            f"create table txx1 (ts timestamp encode 'disabled' compress 'disabled' level 'h', f int compress 'lz4') tags(t int)"
        )

        count = 0
        subTb1 = "txx1_sub1"
        subTb2 = "txx1_sub2"

        tdSql.execute(f"create table {subTb1} using {stb1} tags(1)")
        tdSql.execute(f"create table {subTb2} using {stb1} tags(2)")

        while count < N:
            ms = 1591200030000 + count
            tdSql.execute(f"insert into {subTb1} values(  {ms} , 1)")

            ms2 = 1591200040000 + count
            tdSql.execute(f"insert into {subTb2} values(  {ms2} , 1)")
            count = count + 1

        count = 0
        tdSql.query(f"select * from {stb1}")
        tdSql.checkRows(M)

        tdSql.execute(f"flush database {db}")

        tdSql.query(f"select * from {stb1}")
        tdSql.checkRows(M)

        L = 8000
        tdSql.execute(f"alter table {stb1} modify column ts encode 'delta-i'")
        tdSql.execute(f"alter table {stb1} modify column f encode 'disabled'")

        while count < N:
            ms = 1591200050000 + count
            tdSql.execute(f"insert into {subTb1} values(  {ms} , 1)")

            ms2 = 1591200060000 + count
            tdSql.execute(f"insert into {subTb2} values(  {ms2} , 1)")
            count = count + 1

        tdSql.query(f"select * from {stb1}")
        tdSql.checkRows(L)

        tdSql.execute(f"flush database {db}")

        tdSql.query(f"select * from {stb1}")
        tdSql.checkRows(L)

        tdSql.execute(f"alter table {stb1} modify column ts encode 'disabled'")

        count = 0
        I = 12000
        while count < N:
            ms = 1591200070000 + count
            tdSql.execute(f"insert into {subTb1} values(  {ms} , 1)")

            ms2 = 1591200080000 + count
            tdSql.execute(f"insert into {subTb2} values(  {ms2} , 1)")
            count = count + 1

        tdSql.query(f"select * from {stb1}")
        tdSql.checkRows(I)

        tdSql.execute(f"flush database {db}")

        tdSql.query(f"select * from {stb1}")
        tdSql.checkRows(I)
