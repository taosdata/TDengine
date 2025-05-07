from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseAlterOption:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_alter_option(self):
        """alter database option

        1. -

        Catalog:
            - Database:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/alter_option.sim

        """

        clusterComCheck.checkDnodes(3)

        tdLog.info(f"============= create database")
        # database_option: {
        #  | BUFFER value       [3~16384, default: 256]
        #  | PAGES value        [64~16384, default: 256]
        #  | CACHEMODEL value   ['node', 'last_row', 'last_value', 'both']
        #  | WAL_FSYNC_PERIOD value        [0 ~ 180000 ms]
        #  | KEEP value         [duration, 365000]
        #  | REPLICA value      [1 | 3]
        #  | WAL_LEVEL value          [1 | 2]

        tdSql.execute(
            f"create database db CACHEMODEL 'both' COMP 0 DURATION 240 WAL_FSYNC_PERIOD 1000 MAXROWS 8000 MINROWS 10 KEEP 1000 S3_KEEPLOCAL 720 PRECISION 'ns' REPLICA 3 WAL_LEVEL 2 VGROUPS 6 SINGLE_STABLE 1"
        )
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}"
        )
        tdLog.info(f"====> dataX_db")

        tdSql.checkRows(3)
        tdSql.checkKeyData("db", 0, "db")
        tdSql.checkKeyData("db", 2, 6)
        tdSql.checkKeyData("db", 3, 0)
        tdSql.checkKeyData("db", 4, 3)
        tdSql.checkKeyData("db", 5, "on")
        tdSql.checkKeyData("db", 6, "240d")
        tdSql.checkKeyData("db", 7, "1000d,1000d,1000d")
        tdSql.checkKeyData("db", 8, 256)
        tdSql.checkKeyData("db", 9, 4)
        tdSql.checkKeyData("db", 10, 256)
        tdSql.checkKeyData("db", 11, 10)
        tdSql.checkKeyData("db", 12, 8000)
        tdSql.checkKeyData("db", 13, 0)
        tdSql.checkKeyData("db", 14, "ns")
        tdSql.checkKeyData("db", 18, "both")
        tdSql.checkKeyData("db", 19, 1)
        tdSql.checkKeyData("db", 20, 2)
        tdSql.checkKeyData("db", 21, 1000)
        tdSql.checkKeyData("db", 22, 3600)
        tdSql.checkKeyData("db", 23, 0)

        # sql show db.vgroups
        # if $tdSql.getData(0,4) == leader then
        #  if $tdSql.getData(0,6) != follower then
        #    return -1
        #  endi
        #  if $tdSql.getData(0,8) != follower then
        #    return -1
        #  endi
        # endi
        # if $tdSql.getData(0,6) == leader then
        #  if $tdSql.getData(0,4) != follower then
        #    return -1
        #  endi
        #  if $tdSql.getData(0,8) != follower then
        #    return -1
        #  endi
        # endi
        # if $tdSql.getData(0,8) == leader then
        #  if $tdSql.getData(0,4) != follower then
        #    return -1
        #  endi
        #  if $tdSql.getData(0,6) != follower then
        #    return -1
        #  endi
        # endi
        #
        # if $tdSql.getData(0,4) != leader then
        #  if $tdSql.getData(0,4) != follower then
        #    return -1
        #  endi
        # endi
        # if $tdSql.getData(0,6) != leader then
        #  if $tdSql.getData(0,6) != follower then
        #    return -1
        #  endi
        # endi
        # if $tdSql.getData(0,8) != leader then
        #  if $tdSql.getData(0,8) != follower then
        #    return -1
        #  endi
        # endi

        tdLog.info(
            f"============== not support modify options: name, create_time, vgroups, ntables"
        )
        tdSql.error(f"alter database db name dba")
        tdSql.error(f'alter database db create_time "2022-03-03 15:08:13.329"')
        tdSql.error(f"alter database db vgroups -1")
        tdSql.error(f"alter database db vgroups 0")
        tdSql.error(f"alter database db vgroups 2")
        tdSql.error(f"alter database db vgroups 20")
        tdSql.error(f"alter database db ntables -1")
        tdSql.error(f"alter database db ntables 0")
        tdSql.error(f"alter database db ntables 1")
        tdSql.error(f"alter database db ntables 10")

        # print ============== modify replica        # TD-14409
        tdSql.error(f"alter database db replica 2")
        tdSql.error(f"alter database db replica 5")
        tdSql.error(f"alter database db replica -1")
        tdSql.error(f"alter database db replica 0")
        # sql alter database db replica 1
        # sql select * from information_schema.ins_databases
        # print replica: $data4_db
        # if $data4_db != 1 then
        #  return -1
        # endi
        # sql alter database db replica 3
        # sql select * from information_schema.ins_databases
        # print replica: $data4_db
        # if $data4_db != 3 then
        #  return -1
        # endi

        # print ============== modify quorum
        # sql alter database db quorum 2
        # sql select * from information_schema.ins_databases
        # print quorum $data5_db
        # if $data5_db != 2 then
        #  return -1
        # endi
        # sql alter database db quorum 1
        # sql select * from information_schema.ins_databases
        # print quorum $data5_db
        # if $data5_db != 1 then
        #  return -1
        # endi

        # sql_error alter database db quorum -1
        # sql_error alter database db quorum 0
        # sql_error alter database db quorum 3
        # sql_error alter database db quorum 4
        # sql_error alter database db quorum 5

        # print ============== modify duration
        tdSql.error(f"alter database db duration 480")
        tdSql.error(f"alter database db duration 360")
        tdSql.error(f"alter database db duration 0")
        tdSql.error(f"alter database db duration 14400  # set over than keep")

        tdLog.info(f"============== modify keep")
        tdSql.execute(f"alter database db keep 2400")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 7, "2400d,2400d,2400d")

        # sql alter database db keep 1000,2000
        # sql select * from information_schema.ins_databases
        # print keep $data7_db
        # if $data7_db != 500,500,500 then
        #  return -1
        # endi

        # sql alter database db keep 40,50
        # sql alter database db keep 30,31
        # sql alter database db keep 20
        # sql_error alter database db keep 10.0
        # sql_error alter database db keep 9
        # sql_error alter database db keep 1
        tdSql.error(f"alter database db keep 0")
        tdSql.error(f"alter database db keep -1")
        # sql_error alter database db keep 365001

        # print ============== modify cache
        # sql_error alter database db cache 12
        # sql_error alter database db cache 1
        # sql_error alter database db cache 60
        # sql_error alter database db cache 50
        # sql_error alter database db cache 20
        # sql_error alter database db cache 3
        # sql_error alter database db cache 129
        # sql_error alter database db cache 300
        # sql_error alter database db cache 0
        # sql_error alter database db cache -1

        # print ============== modify blocks
        # sql alter database db blocks 3
        # sql select * from information_schema.ins_databases
        # print blocks $data9_db
        # if $data9_db != 3 then
        #  return -1
        # endi
        # sql alter database db blocks 11
        # sql select * from information_schema.ins_databases
        # print blocks $data9_db
        # if $data9_db != 11 then
        #  return -1
        # endi

        # sql alter database db blocks 40
        # sql alter database db blocks 30
        # sql alter database db blocks 20
        # sql alter database db blocks 10
        # sql_error alter database db blocks 2
        # sql_error alter database db blocks 1
        # sql_error alter database db blocks 0
        # sql_error alter database db blocks -1
        # sql_error alter database db blocks 10001

        tdLog.info(f"============== modify minrows")
        tdSql.error(f"alter database db minrows 8")
        tdSql.error(f"alter database db minrows 8000000")
        tdSql.error(f"alter database db minrows 8001000")

        tdLog.info(f"============== modify maxrows")
        tdSql.error(f"alter database db maxrows 10000001")
        tdSql.error(f"alter database db maxrows 20000000")
        tdSql.error(f"alter database db maxrows 11  # equal minrows")
        tdSql.error(f"alter database db maxrows 10  # little than minrows")

        tdLog.info(f"============== step wal_level")
        tdSql.execute(f"alter database db wal_level 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 20, 1)

        tdSql.execute(f"alter database db wal_level 2")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 20, 2)

        tdSql.error(f"alter database db wal_level 0     # TD-14436")
        tdSql.error(f"alter database db wal_level 3")
        tdSql.error(f"alter database db wal_level 100")
        tdSql.error(f"alter database db wal_level -1")

        tdLog.info(f"============== modify wal_fsync_period")
        tdSql.execute(f"alter database db wal_fsync_period 2000")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 21, 2000)

        tdSql.execute(f"alter database db wal_fsync_period 500")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 21, 500)

        tdSql.execute(f"alter database db wal_fsync_period 0")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 21, 0)

        tdSql.error(f"alter database db wal_fsync_period 180001")
        tdSql.error(f"alter database db wal_fsync_period -1")

        tdLog.info(f"============== modify comp")
        tdSql.error(f"alter database db comp 1")
        tdSql.error(f"alter database db comp 2")
        tdSql.error(f"alter database db comp 1")
        tdSql.error(f"alter database db comp 0")
        tdSql.error(f"alter database db comp 3")
        tdSql.error(f"alter database db comp 4")
        tdSql.error(f"alter database db comp 5")
        tdSql.error(f"alter database db comp -1")

        tdLog.info(f"============== modify cachelast [0, 1, 2, 3]")
        tdSql.execute(f"alter database db cachemodel 'last_value'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 18, "last_value")

        tdSql.execute(f"alter database db cachemodel 'last_row'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 18, "last_row")

        tdSql.execute(f"alter database db cachemodel 'none'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 18, "none")

        tdSql.execute(f"alter database db cachemodel 'last_value'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 18, "last_value")

        tdSql.execute(f"alter database db cachemodel 'both'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 18, "both")

        tdSql.error(f"alter database db cachelast 4")
        tdSql.error(f"alter database db cachelast 10")
        tdSql.error(f"alter database db cachelast 'other'")

        tdLog.info(f"============== modify precision")
        tdSql.error(f"alter database db precision 'ms'")
        tdSql.error(f"alter database db precision 'us'")
        tdSql.error(f"alter database db precision 'ns'")
        tdSql.error(f"alter database db precision 'ys'")
        tdSql.error(f"alter database db prec 'xs'")
