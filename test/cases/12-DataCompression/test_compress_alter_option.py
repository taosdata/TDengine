from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestCompressAlterOption:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_compress_alter_option(self):
        """Compress: alter options

        1. Create a table containing data types including BOOL, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, and BINARY;
        2. insert records and query the results
        3. Modify compression settings (compression option, level, and encoding method), including disabling compression entirely, then query the results
        4. Execute FLUSH DATABASE and restart the taosdservice
        5. Insert new data again and continue querying the results


        Catalog:
            - Compress

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compress/compress.sim
            - 2025-4-28 Simon Guan Migrated from tsim/compress/compress2.sim
            - 2025-4-28 Simon Guan Migrated from tsim/compress/compressDisable.sim

        """

        self.Compress()
        tdStream.dropAllStreamsAndDbs()
        self.Compress2()
        tdStream.dropAllStreamsAndDbs()
        self.CompressDisable()
        tdStream.dropAllStreamsAndDbs()

    def Compress(self):
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

    def Compress2(self):
        tdLog.info(f"============================ dnode1 start")
        i = 0
        dbPrefix = "db"
        tbPrefix = "tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        stb = "teststb"

        N = 2000

        tdLog.info(f"=============== step1")

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {tb} (ts timestamp, b bool, t tinyint, s smallint, i int, big bigint, str binary(256))"
        )

        tdSql.execute(f"alter table {tb} add column f bool")
        tdSql.query(f"desc {tb}")
        tdSql.execute(f"alter table {tb} drop column f")
        tdSql.query(f"desc {tb}")

        tdSql.error(f"create table txx (ts timestamp compress 'xxx', f int)")
        tdSql.execute(f"create table txx (ts timestamp compress 'disabled', f int)")

        tdSql.error(f"alter table {tb} modify column b level 'i'")
        tdSql.execute(f"alter table {tb} modify column b level 'l'")
        tdSql.error(f"alter table {tb} modify column b level 'l' # already exist")
        tdSql.execute(f"alter table {tb} modify column b level 'm'")
        tdSql.error(f"alter table {tb} modify column b level 'l' # already exist")

        tdSql.execute(f"alter table {tb} modify column b compress 'lz4'")
        tdSql.execute(f"alter table {tb} modify column b compress 'xz'")
        tdSql.execute(f"alter table {tb} modify column b compress 'zstd'")
        tdSql.error(f"alter table {tb} modify column b compress 'tsz'")

        count = 0
        while count < N:
            ms = 1591200000000 + count
            tdSql.execute(
                f"insert into {tb} values(  {ms} , 1, 0, {count} , {count} , {count} ,'it is a string')"
            )
            count = count + 1

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step2")
        i = 1
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table {tb} (ts timestamp, f float, d double, str binary(256))"
        )

        count = 0
        while count < N:
            ms = 1591286400000 + count
            tdSql.execute(
                f"insert into {tb} values( {ms} , {count} , {count} ,'it is a string')"
            )
            count = count + 1

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step3")
        i = 2
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table {tb} (ts timestamp, b bool, t tinyint, s smallint, i int, big bigint, f float, d double, str binary(256))"
        )

        count = 0
        while count < N:
            ms = 1591372800000 + count
            tdSql.execute(
                f"insert into {tb} values( {ms} , 1 , 0 , {count} , {count} , {count} , {count} , {count} ,'it is a string')"
            )
            count = count + 1

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step4")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step5")

        i = 0
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()} points")
        tdSql.checkRows(N)

        i = 1
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()} points")
        tdSql.checkRows(N)

        i = 2
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()} points")
        tdSql.checkRows(N)

        # super table
        tdSql.execute(
            f"create table {stb} (ts timestamp, b bool, t tinyint, s smallint, i int, big bigint, str binary(256), f float, d double) tags(t1 int, t2 int)"
        )

        tdSql.query(f"desc {stb}")
        tdSql.error(f"alter table {stb} modify column b level 'i'")
        tdSql.execute(f"alter table {stb} modify column b level 'l'")
        tdSql.error(f"alter table {stb} modify column b level 'l' # already exist")
        tdSql.execute(f"alter table {stb} modify column b level 'm'")
        tdSql.error(f"alter table {stb} modify column b level 'l' # already exist")
        tdSql.query(f"desc {stb}")

        tdSql.execute(f"alter table {stb} modify column b compress 'lz4'")
        tdSql.execute(f"alter table {stb} modify column b compress 'xz'")
        tdSql.execute(f"alter table {stb} modify column b compress 'zstd'")
        tdSql.error(f"alter table {stb} modify column b compress 'tsz'")
        tdSql.execute(f"alter table {stb} modify column b compress 'zlib'")
        tdSql.query(f"desc {stb}")

        tdSql.error(f"alter table {stb} modify column f compress 'lz4'")
        tdSql.execute(f"alter table {stb} modify column f compress 'disabled'")
        tdSql.query(f"desc {stb}")
        tdSql.execute(f"alter table {stb} modify column f compress 'tsz'")
        tdSql.query(f"desc {stb}")
        tdSql.execute(f"alter table {stb} modify column f compress 'zlib'")
        tdSql.query(f"desc {stb}")
        tdSql.execute(f"alter table {stb} modify column f compress 'zstd'")

        tdSql.execute(f"alter table {stb} modify column f compress 'zstd' level 'h'")
        tdSql.error(f"alter table {stb} modify column f compress 'zstd' level 'h'")

        tdSql.execute(f"alter table {stb} modify column f compress 'lz4' level 'h'")
        tdSql.error(f"alter table {stb} modify column f compress 'lz4' level 'h'")

        tdSql.execute(f"alter table {stb} modify column f level 'low'")
        tdSql.error(f"alter table {stb} modify column f compress 'lz4'")

        tdSql.error(f"alter table {stb} modify column f compress 'lz4' level 'low'")

        tdSql.execute(f"alter table {stb} modify column f compress 'zstd' level 'h'")

        tdSql.error(f"alter table {stb} modify column f compress 'zstd'")
        tdSql.error(f"alter table {stb} modify column f level 'h'")

        tdSql.execute(f"alter table {stb} modify column f compress 'lz4'")

        tdSql.error(
            f"alter table {stb} modify column d compress 'lz4' # same with init"
        )
        tdSql.execute(f"alter table {stb} modify column d compress 'disabled'")
        tdSql.query(f"desc {stb}")
        tdSql.execute(f"alter table {stb} modify column d compress 'tsz'")
        tdSql.query(f"desc {stb}")

        # from compress_col.sim
        tdSql.error(f"create table txx (ts timestamp compress 'xxx', f int)")
        tdSql.error(
            f"create table txx (ts timestamp compress 'disabled' level 'xxx', f int)"
        )
        tdSql.error(
            f"create table txx (ts timestamp compress 'disabled' level 'h', f int compress 'tsz')"
        )
        tdSql.error(
            f"create table txx (ts timestamp compress 'disabled' level 'h', f int compress 'tsz')"
        )
        tdSql.execute(
            f"create table txx1 (ts timestamp compress 'disabled' level 'h', f int compress 'lz4')"
        )
        tdSql.execute(
            f"create table txx2 (ts timestamp compress 'disabled' level 'h', f int compress 'zlib')"
        )
        tdSql.execute(
            f"create table txx3 (ts timestamp compress 'disabled' level 'h', f int compress 'xz')"
        )
        tdSql.execute(
            f"create table txx4 (ts timestamp compress 'disabled' level 'h', f int compress 'zstd')"
        )

        tdSql.execute(f"alter table txx4 add column tt int compress 'xz' level 'h'")
        tdSql.execute(f"alter table txx4 drop column tt")

        tdSql.execute(f"create stable sp(ts timestamp, c int) tags(t int)")
        tdSql.execute(f"alter table sp add column c1 int compress 'zstd'")
        tdSql.execute(f"alter table sp drop column c1")

        tdSql.execute(f"alter stable sp add column c1 int compress 'zstd'")
        tdSql.execute(f"alter stable sp drop column c1")

    def CompressDisable(self):
        tdLog.info(f"============================ dnode1 start")
        i = 0
        dbPrefix = "db"
        tbPrefix = "tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        stb = "teststb"

        N = 2000

        tdLog.info(f"=============== step1")

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {tb} (ts timestamp, b bool, t tinyint, s smallint, i int, big bigint, str binary(256))"
        )

        tdSql.execute(f"alter table {tb} add column f bool")
        tdSql.query(f"desc {tb}")
        tdSql.execute(f"alter table {tb} drop column f")
        tdSql.query(f"desc {tb}")

        tdSql.error(f"create table txx (ts timestamp compress 'xxx', f int)")
        tdSql.execute(f"create table txx (ts timestamp compress 'disabled', f int)")

        tdSql.error(f"alter table {tb} modify column b level 'i'")
        tdSql.execute(f"alter table {tb} modify column b level 'l'")
        tdSql.error(f"alter table {tb} modify column b level 'l' # already exist")
        tdSql.execute(f"alter table {tb} modify column b level 'm'")
        tdSql.error(f"alter table {tb} modify column b level 'l' # already exist")

        tdSql.execute(f"alter table {tb} modify column b compress 'lz4'")
        tdSql.execute(f"alter table {tb} modify column b compress 'xz'")
        tdSql.execute(f"alter table {tb} modify column b compress 'zstd'")
        tdSql.error(f"alter table {tb} modify column b compress 'tsz'")

        count = 0
        while count < N:
            ms = 1591200000000 + count
            tdSql.execute(
                f"insert into {tb} values(  {ms} , 1, 0, {count} , {count} , {count} ,'it is a string')"
            )
            count = count + 1

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step2")
        i = 1
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table {tb} (ts timestamp, f float, d double, str binary(256))"
        )

        count = 0
        while count < N:
            ms = 1591286400000 + count
            tdSql.execute(
                f"insert into {tb} values( {ms} , {count} , {count} ,'it is a string')"
            )
            count = count + 1

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step3")
        i = 2
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table {tb} (ts timestamp, b bool, t tinyint, s smallint, i int, big bigint, f float, d double, str binary(256))"
        )

        count = 0
        while count < N:
            ms = 1591372800000 + count
            tdSql.execute(
                f"insert into {tb} values( {ms} , 1 , 0 , {count} , {count} , {count} , {count} , {count} ,'it is a string')"
            )
            count = count + 1

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step4")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step5")

        i = 0
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()} points")
        tdSql.checkRows(N)

        i = 1
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()} points")
        tdSql.checkRows(N)

        i = 2
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()} points")
        tdSql.checkRows(N)

        # super table
        tdSql.execute(
            f"create table {stb} (ts timestamp, b bool, t tinyint, s smallint, i int, big bigint, str binary(256), f float, d double) tags(t1 int, t2 int)"
        )

        tdSql.query(f"desc {stb}")
        tdSql.error(f"alter table {stb} modify column b level 'i'")
        tdSql.execute(f"alter table {stb} modify column b level 'l'")
        tdSql.error(f"alter table {stb} modify column b level 'l' # already exist")
        tdSql.execute(f"alter table {stb} modify column b level 'm'")
        tdSql.error(f"alter table {stb} modify column b level 'l' # already exist")
        tdSql.query(f"desc {stb}")

        tdSql.execute(f"alter table {stb} modify column b compress 'lz4'")
        tdSql.execute(f"alter table {stb} modify column b compress 'xz'")
        tdSql.execute(f"alter table {stb} modify column b compress 'zstd'")
        tdSql.error(f"alter table {stb} modify column b compress 'tsz'")
        tdSql.execute(f"alter table {stb} modify column b compress 'zlib'")
        tdSql.query(f"desc {stb}")

        tdSql.error(f"alter table {stb} modify column f compress 'lz4'")
        tdSql.execute(f"alter table {stb} modify column f compress 'disabled'")
        tdSql.query(f"desc {stb}")
        tdSql.execute(f"alter table {stb} modify column f compress 'tsz'")
        tdSql.query(f"desc {stb}")
        tdSql.execute(f"alter table {stb} modify column f compress 'zlib'")
        tdSql.query(f"desc {stb}")
        tdSql.execute(f"alter table {stb} modify column f compress 'zstd'")

        tdSql.execute(f"alter table {stb} modify column f compress 'zstd' level 'h'")
        tdSql.error(f"alter table {stb} modify column f compress 'zstd' level 'h'")

        tdSql.execute(f"alter table {stb} modify column f compress 'lz4' level 'h'")
        tdSql.error(f"alter table {stb} modify column f compress 'lz4' level 'h'")

        tdSql.execute(f"alter table {stb} modify column f level 'low'")
        tdSql.error(f"alter table {stb} modify column f compress 'lz4'")

        tdSql.error(f"alter table {stb} modify column f compress 'lz4' level 'low'")

        tdSql.execute(f"alter table {stb} modify column f compress 'zstd' level 'h'")

        tdSql.error(f"alter table {stb} modify column f compress 'zstd'")
        tdSql.error(f"alter table {stb} modify column f level 'h'")

        tdSql.execute(f"alter table {stb} modify column f compress 'lz4'")

        tdSql.error(
            f"alter table {stb} modify column d compress 'lz4' # same with init"
        )
        tdSql.execute(f"alter table {stb} modify column d compress 'disabled'")
        tdSql.query(f"desc {stb}")
        tdSql.execute(f"alter table {stb} modify column d compress 'tsz'")
        tdSql.query(f"desc {stb}")

        # from compress_col.sim
        tdSql.error(f"create table txx (ts timestamp compress 'xxx', f int)")
        tdSql.error(
            f"create table txx (ts timestamp compress 'disabled' level 'xxx', f int)"
        )
        tdSql.error(
            f"create table txx (ts timestamp compress 'disabled' level 'h', f int compress 'tsz')"
        )
        tdSql.error(
            f"create table txx (ts timestamp compress 'disabled' level 'h', f int compress 'tsz')"
        )
        tdSql.execute(
            f"create table txx1 (ts timestamp compress 'disabled' level 'h', f int compress 'lz4')"
        )
        tdSql.execute(
            f"create table txx2 (ts timestamp compress 'disabled' level 'h', f int compress 'zlib')"
        )
        tdSql.execute(
            f"create table txx3 (ts timestamp compress 'disabled' level 'h', f int compress 'xz')"
        )
        tdSql.execute(
            f"create table txx4 (ts timestamp compress 'disabled' level 'h', f int compress 'zstd')"
        )

        tdSql.execute(f"alter table txx4 add column tt int compress 'xz' level 'h'")
        tdSql.execute(f"alter table txx4 drop column tt")

        tdSql.execute(f"create stable sp(ts timestamp, c int) tags(t int)")
        tdSql.execute(f"alter table sp add column c1 int compress 'zstd'")
        tdSql.execute(f"alter table sp drop column c1")

        tdSql.execute(f"alter stable sp add column c1 int compress 'zstd'")
        tdSql.execute(f"alter stable sp drop column c1")
