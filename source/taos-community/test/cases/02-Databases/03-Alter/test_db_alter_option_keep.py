from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseAlterOptionKeep:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_alter_option_keep(self):
        """Alter keep

        1. Use invalid input to alter the KEEP option
        2. Verify results after changing KEEP
        3. Add or drop columns on the super table
        4. Insert data
        5. Check results
        6. Repeat steps 3-5 several times


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/alter.sim
            - 2025-5-6 Simon Guan Migrated from tsim/parser/alter__for_community_version.sim

        """

        dbPrefix = "m_alt_db"
        tbPrefix = "m_alt_tb"
        mtPrefix = "m_alt_mt"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} duration 3 keep 20")
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "20d,20d,20d")

        tdSql.error(f'alter database {db} keep "20"')
        tdSql.error(f'alter database {db} keep "20","20","20"')
        tdSql.error(f"alter database {db} keep 0")
        tdSql.error(f"alter database {db} keep 20.0")
        tdSql.error(f"alter database {db} keep 20.0,20.0,20.0")
        tdSql.error(f"alter database {db} keep 0,0,0")
        tdSql.error(f"alter database {db} keep 3")
        tdSql.error(f"alter database {db} keep -1,-1,-1")
        tdSql.execute(f"alter database {db} keep 20,20")
        tdSql.error(f"alter database {db} keep 8,9,9")
        tdSql.error(f"alter database {db} keep 20,20,19")
        tdSql.error(f"alter database {db} keep 20,19,20")
        tdSql.error(f"alter database {db} keep 20,19,19")
        tdSql.error(f"alter database {db} keep 20,19,18")
        tdSql.error(f"alter database {db} keep 20,20,20,20")
        tdSql.error(f"alter database {db} keep 365001,365001,365001")
        tdSql.error(f"alter database {db} keep 365001")
        tdSql.execute(f"alter database {db} keep 20")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "20d,20d,20d")

        tdSql.execute(f"alter database {db} keep 10")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "10d,10d,10d")

        tdSql.execute(f"alter database {db} keep 11")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,11d,11d")

        tdSql.execute(f"alter database {db} keep 13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "13d,13d,13d")

        tdSql.execute(f"alter database {db} keep 365000")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "365000d,365000d,365000d")

        ##### alter table test, simeplest case
        tdSql.execute(f"create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute(f"insert into tb values (now, 1, 1, 1)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(1)

        tdSql.execute(f"alter table tb drop column c3")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.execute(f"alter table tb add column c3 nchar(4)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(1)
        tdSql.checkData(0, 3, None)

        tdSql.execute(f"insert into tb values (now, 2, 2, 'taos')")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(2)

        tdLog.info(f"tdSql.getData(0,3) = {tdSql.getData(0,3)}")
        tdSql.checkData(0, 3, "taos")

        tdSql.execute(f"drop table tb")

        ##### alter metric test, simplest case
        tdSql.execute(
            f"create table mt (ts timestamp, c1 int, c2 int, c3 int) tags (t1 int)"
        )
        tdSql.execute(f"create table tb using mt tags(1)")
        tdSql.execute(f"insert into tb values (now, 1, 1, 1)")
        tdSql.execute(f"alter table mt drop column c3")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.execute(f"alter table mt add column c3 nchar(4)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 3, None)

        tdSql.execute(f"insert into tb values (now, 2, 2, 'taos')")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, "taos")
        tdSql.checkData(1, 3, None)

        tdSql.execute(f"drop table tb")
        tdSql.execute(f"drop table mt")

        ## [TBASE272]
        tdSql.execute(f"create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute(f"insert into tb values (now, 1, 1, 1)")
        tdSql.execute(f"alter table tb drop column c3")
        tdSql.execute(f"alter table tb add column c3 nchar(5)")
        tdSql.execute(f"insert into tb values(now, 2, 2, 'taos')")
        tdSql.execute(f"drop table tb")
        tdSql.execute(
            f"create table mt (ts timestamp, c1 int, c2 int, c3 int) tags (t1 int)"
        )
        tdSql.execute(f"create table tb using mt tags(1)")
        tdSql.execute(f"insert into tb values (now, 1, 1, 1)")
        tdSql.execute(f"alter table mt drop column c3")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table tb")
        tdSql.execute(f"drop table mt")

        ### ALTER TABLE WHILE STREAMING [TBASE271]
        # sql create table tb1 (ts timestamp, c1 int, c2 nchar(5), c3 int)
        # sql create table strm as select count(*), avg(c1), first(c2), sum(c3) from tb1 interval(2s)
        # sql select * from strm
        # if $rows != 0 then
        #  return -1
        # endi
        # sql insert into tb1 values (now, 1, 'taos', 1)
        # sql select * from strm
        # print rows = $rows
        # if $rows != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,4) != 1 then
        #  return -1
        # endi
        # sql alter table tb1 drop column c3
        # sql insert into tb1 values (now, 2, 'taos')
        # sql select * from strm
        # if $rows != 2 then
        #   return -1
        # endi
        # if $tdSql.getData(0,4) != 1 then
        #  return -1
        # endi
        # sql alter table tb1 add column c3 int
        # sql insert into tb1 values (now, 3, 'taos', 3);
        # sql select * from strm
        # if $rows != 3 then
        #   return -1
        # endi
        # if $tdSql.getData(0,4) != 1 then
        #  return -1
        # endi

        ## ALTER TABLE AND INSERT BY COLUMNS
        tdSql.execute(f"create table mt (ts timestamp, c1 int, c2 int) tags(t1 int)")
        tdSql.execute(f"create table tb using mt tags(0)")
        tdSql.execute(f"insert into tb values (now-1m, 1, 1)")
        tdSql.execute(f"alter table mt drop column c2")
        tdSql.error(f"insert into tb (ts, c1, c2) values (now, 2, 2)")
        tdSql.execute(f"insert into tb (ts, c1) values (now, 2)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"alter table mt add column c2 int")
        tdSql.execute(f"insert into tb (ts, c2) values (now, 3)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 2, 3)

        ## ALTER TABLE AND IMPORT
        tdSql.execute(f"drop database {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table mt (ts timestamp, c1 int, c2 nchar(7), c3 int) tags (t1 int)"
        )
        tdSql.execute(f"create table tb using mt tags(1)")
        tdSql.execute(
            f"insert into tb values ('2018-11-01 16:30:00.000', 1, 'insert', 1)"
        )
        tdSql.execute(f"alter table mt drop column c3")

        tdSql.execute(f"insert into tb values ('2018-11-01 16:29:59.000', 1, 'insert')")
        tdSql.execute(f"import into tb values ('2018-11-01 16:29:59.000', 1, 'import')")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "insert")

        tdSql.execute(f"alter table mt add column c3 nchar(4)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkData(0, 3, None)

        tdLog.info(f"============================>TD-3366 TD-3486")
        tdSql.execute(
            f"insert into td_3366(ts, c3, c1) using mt(t1) tags(911) values('2018-1-1 11:11:11', 'new1', 12);"
        )
        tdSql.execute(
            f"insert into td_3486(ts, c3, c1) using mt(t1) tags(-12) values('2018-1-1 11:11:11', 'new1', 12);"
        )
        tdSql.execute(
            f"insert into ttxu(ts, c3, c1) using mt(t1) tags('-121') values('2018-1-1 11:11:11', 'new1', 12);"
        )

        tdSql.execute(
            f"insert into tb(ts, c1, c3) using mt(t1) tags(123) values('2018-11-01 16:29:58.000', 2, 'port')"
        )

        tdSql.execute(
            f"insert into tb values ('2018-11-01 16:29:58.000', 2, 'import', 3)"
        )
        tdSql.execute(
            f"import into tb values ('2018-11-01 16:29:58.000', 2, 'import', 3)"
        )
        tdSql.execute(
            f"import into tb values ('2018-11-01 16:39:58.000', 2, 'import', 3)"
        )
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(4)

        tdSql.checkData(0, 3, 3)

        ##### ILLEGAL OPERATIONS

        # try dropping columns that are defined in metric
        tdSql.error(f"alter table tb drop column c1;")

        # try dropping primary key
        tdSql.error(f"alter table mt drop column ts;")

        # try modifying two columns in a single statement
        tdSql.error(f"alter table mt add column c5 nchar(3) c6 nchar(4)")

        # duplicate columns
        tdSql.error(f"alter table mt add column c1 int")

        # drop non-existing columns
        tdSql.error(f"alter table mt drop column c9")

        # merge other cases
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} duration 3 keep 20,20,20")
        tdSql.execute(f"use {db}")

        tdSql.error(f'alter database {db} keep "20"')
        tdSql.error(f'alter database {db} keep "20","20","20"')
        tdSql.error(f"alter database {db} keep 20,19")
        tdSql.error(f"alter database {db} keep 20.0")
        tdSql.error(f"alter database {db} keep 20.0,20.0,20.0")
        tdSql.error(f"alter database {db} keep 0,0,0")
        tdSql.error(f"alter database {db} keep -1,-1,-1")
        tdSql.error(f"alter database {db} keep 8,20")
        tdSql.error(f"alter database {db} keep 8,9,9")
        tdSql.error(f"alter database {db} keep 20,20,19")
        tdSql.error(f"alter database {db} keep 20,19,20")
        tdSql.error(f"alter database {db} keep 20,19,19")
        tdSql.error(f"alter database {db} keep 20,19,18")
        tdSql.error(f"alter database {db} keep 20,20,20,20")
        tdSql.error(f"alter database {db} keep 365001,365001,365001")
        tdSql.execute(f"alter database {db} keep 21")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "21d,21d,21d")

        tdSql.execute(f"alter database {db} keep 11,12")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "11d,12d,12d")

        tdSql.execute(f"alter database {db} keep 20,20,20")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "20d,20d,20d")

        tdSql.execute(f"alter database {db} keep 10,10,10")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "10d,10d,10d")

        tdSql.execute(f"alter database {db} keep 10,10,11")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "10d,10d,11d")

        tdSql.execute(f"alter database {db} keep 11,12,13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.checkData(2, 7, "11d,12d,13d")

        tdSql.execute(f"alter database {db} keep 365000,365000,365000")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "365000d,365000d,365000d")
