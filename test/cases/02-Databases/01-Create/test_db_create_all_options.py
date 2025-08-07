import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseCreateAllOptions:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_create_all_options(self):
        """Options

        1. create database using all the available options
        2. query information_schema.ins_databases to confirm that the options are displayed correctly
        3. test the ranges of each option.

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/create_all_options.sim

        """

        clusterComCheck.checkDnodes(3)

        tdLog.info(f"============= create database with all options")
        # database_option: {
        #  | BUFFER value       [3~16384, default: 256]
        #  | PAGES value        [64~16384, default: 256]
        #  | PAGESIZE value     [1~16384, default: 4]
        #  | CACHEMODEL value   ['node', 'last_row', 'last_value', 'both', default: 'node']
        #  | COMP               [0 | 1 | 2, default: 2]
        #  | DURATION value         [60m ~ min(3650d,keep), default: 10d, unit may be minut/hour/day]
        #  | WAL_FSYNC_PERIOD value        [0 ~ 180000 ms, default: 3000]
        #  | MAXROWS value      [200~10000, default: 4096]
        #  | MINROWS value      [10~1000, default: 100]
        #  | KEEP value         [max(1d ~ 365000d), default: 1d, unit may be minut/hour/day]
        #  | PRECISION          ['ms' | 'us' | 'ns', default: ms]
        #  | REPLICA value      [1 | 3, default: 1]
        #  | WAL_LEVEL value          [0 | 1 | 2, default: 1]
        #  | VGROUPS value      [default: 2]
        #  | SINGLE_STABLE      [0 | 1, default: ]
        #
        # $data0_db  : name
        # $data1_db  : create_time
        # $data2_db  : vgroups
        # $data3_db  : ntables
        # $data4_db  : replica
        # $data6_db  : duration
        # $data7_db  : keep
        # $data10_db : minrows
        # $data11_db : maxrows
        # $data12_db : wal_level
        # $data13_db : fsync
        # $data14_db : comp
        # $data15_db : cachelast
        # $data16_db : precision

        tdLog.info(f"====> create database db, with default")
        tdSql.execute(f"create database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(3)

        tdSql.checkKeyData("db", 0, "db")
        tdSql.checkKeyData("db", 2, 2)
        tdSql.checkKeyData("db", 3, 0)
        tdSql.checkKeyData("db", 4, 1)
        tdSql.checkKeyData("db", 5, "on")
        tdSql.checkKeyData("db", 6, "10d")
        tdSql.checkKeyData("db", 7, "3650d,3650d,3650d")
        tdSql.checkKeyData("db", 8, 256)
        tdSql.checkKeyData("db", 9, 4)
        tdSql.checkKeyData("db", 10, 256)
        tdSql.checkKeyData("db", 11, 100)
        tdSql.checkKeyData("db", 12, 4096)
        tdSql.checkKeyData("db", 13, 2)
        tdSql.checkKeyData("db", 14, "ms")
        tdSql.checkKeyData("db", 18, "none")
        tdSql.checkKeyData("db", 20, 1)
        tdSql.checkKeyData("db", 21, 3000)
        tdSql.checkKeyData("db", 30, "525600m")
        tdSql.checkKeyData("db", 31, 1)

        tdSql.execute(f"drop database db")

        # print ====> BLOCKS value       [3~1000, default: 6]
        # sql create database db BLOCKS 3
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data9_db != 3 then
        #  return -1
        # endi
        # sql drop database db

        # sql create database db BLOCKS 1000
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data9_db != 1000 then
        #  return -1
        # endi
        # sql drop database db
        # sql_error create database db BLOCKS 2
        # sql_error create database db BLOCKS 0
        # sql_error create database db BLOCKS -1

        # print ====> CACHE value [default: 16]
        # sql create database db CACHE 1
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data8_db != 1 then
        #  return -1
        # endi
        # sql drop database db

        # sql create database db CACHE 128
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data8_db != 128 then
        #  return -1
        # endi
        # sql drop database db

        tdLog.info(f"====> CACHEMODEL value [0, 1, 2, 3, default: 0]")
        tdSql.execute(f"create database db CACHEMODEL 'last_row'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 18, "last_row")

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db CACHEMODEL 'last_value'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 18, "last_value")

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db CACHEMODEL 'both'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 18, "both")

        tdSql.execute(f"drop database db")
        tdSql.error(f"create database db CACHEMODEL 'other'")
        tdSql.error(f"create database db CACHEMODEL '-1'")

        tdLog.info(f"====> COMP [0 | 1 | 2, default: 2]")
        tdSql.execute(f"create database db COMP 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 13, 1)

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db COMP 0")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 13, 0)

        tdSql.execute(f"drop database db")
        tdSql.error(f"create database db COMP 3")
        tdSql.error(f"create database db COMP -1")

        # print ====> DURATION value [60m ~ min(3650d,keep), default: 10d, unit may be minut/hour/day]
        # print ====> KEEP value [max(1d ~ 365000d), default: 1d, unit may be minut/hour/day]
        # sql create database db DURATION 60m KEEP 60m
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data6_db != 60 then
        #  return -1
        # endi
        # if $data7_db != 60,60,60 then
        #  return -1
        # endi
        # sql drop database db
        # sql create database db DURATION 60m KEEP 1d
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data6_db != 60 then
        #  return -1
        # endi
        # if $data7_db != 1440,1440,1440 then
        #  return -1
        # endi
        # sql create database db DURATION 3650d KEEP 365000d
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data6_db != 5256000 then
        #  return -1
        # endi
        # if $data7_db != 525600000,525600000,525600000 then
        #  return -1
        # endi
        # sql drop database db
        # sql_error create database db DURATION -59m
        # sql_error create database db DURATION 59m
        # sql_error create database db DURATION 5256001m
        # sql_error create database db DURATION 3651d
        # sql_error create database db KEEP -59m
        # sql_error create database db KEEP 14399m
        # sql_error create database db KEEP 525600001m
        # sql_error create database db KEEP 365001d

        tdLog.info(f"====> WAL_FSYNC_PERIOD value [0 ~ 180000 ms, default: 3000]")
        tdSql.execute(f"create database db WAL_FSYNC_PERIOD 0")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 21, 0)

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db WAL_FSYNC_PERIOD 180000")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 21, 180000)

        tdSql.execute(f"drop database db")
        tdSql.error(f"create database db WAL_FSYNC_PERIOD 180001")
        tdSql.error(f"create database db WAL_FSYNC_PERIOD -1")

        tdLog.info(
            f"====> MAXROWS value [200~10000, default: 4096], MINROWS value [10~1000, default: 100]"
        )
        tdSql.execute(f"create database db MAXROWS 10000 MINROWS 1000")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 11, 1000)
        tdSql.checkKeyData("db", 12, 10000)

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db MAXROWS 200 MINROWS 10")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 11, 10)
        tdSql.checkKeyData("db", 12, 200)

        tdSql.execute(f"drop database db")
        tdSql.error(f"create database db MAXROWS -1")
        tdSql.error(f"create database db MAXROWS 0")
        tdSql.error(f"create database db MAXROWS 199")
        tdSql.error(f"create database db MAXROWS 10000001")
        tdSql.error(f"create database db MINROWS -1")
        tdSql.error(f"create database db MINROWS 0")
        tdSql.error(f"create database db MINROWS 9")
        tdSql.error(f"create database db MINROWS 1000001")
        tdSql.error(f"create database db MAXROWS 500 MINROWS 1000")

        tdSql.execute(f"create database db PRECISION 'us'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 14, "us")

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db PRECISION 'ns'")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 14, "ns")

        tdSql.execute(f"drop database db")
        tdSql.error(f"create database db PRECISION 'as'")
        tdSql.error(f"create database db PRECISION -1")

        tdLog.info(f"====> QUORUM value [1 | 2, default: 1] 3.0 not support this item")
        # sql_error create database db QUORUM 2
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data5_db != 2 then
        #  return -1
        # endi
        # sql drop database db

        # sql create database db QUORUM 1
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $data5_db != 1 then
        #  return -1
        # endi
        # sql drop database db
        tdSql.error(f"create database db QUORUM 1")
        tdSql.error(f"create database db QUORUM 2")
        tdSql.error(f"create database db QUORUM 3")
        tdSql.error(f"create database db QUORUM 0")
        tdSql.error(f"create database db QUORUM -1")

        tdLog.info(f"====> REPLICA value [1 | 3, default: 1]")
        tdSql.execute(f"create database db REPLICA 3")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 4, 3)

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db REPLICA 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 4, 1)

        tdSql.execute(f"drop database db")
        # sql_error create database db REPLICA 2
        tdSql.error(f"create database db REPLICA 0")
        tdSql.error(f"create database db REPLICA -1")
        tdSql.error(f"create database db REPLICA 4")

        # print ====> TTL value [1d ~ , default: 1]
        # sql create database db TTL 1
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $dataXX_db != 1 then
        #  return -1
        # endi
        # sql drop database db

        # sql create database db TTL 10
        # sql select * from information_schema.ins_databases
        # print $data0_db $data1_db $data2_db $data3_db $data4_db $data5_db $data6_db $data7_db $data8_db $data9_db $data10_db $data11_db $data12_db $data13_db $data14_db $data15_db $data16_db $data17_db
        # if $dataXX_db != 10 then
        #  return -1
        # endi
        # sql drop database db
        # sql_error create database db TTL 0
        # sql_error create database db TTL -1

        tdLog.info(f"====> WAL_LEVEL value [1 | 2, default: 1]")
        tdSql.execute(f"create database db WAL_LEVEL 2")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 20, 2)

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db WAL_LEVEL 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 20, 1)

        tdSql.execute(f"drop database db")
        tdSql.error(f"create database db WAL_LEVEL 3")
        tdSql.error(f"create database db WAL_LEVEL -1")
        # sql_error create database db WAL_LEVEL 0

        tdLog.info(f"====> VGROUPS value [1~4096, default: 2]")
        tdSql.execute(f"create database db VGROUPS 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 2, 1)

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db VGROUPS 16")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("db", 2, 16)

        tdSql.execute(f"drop database db")
        tdSql.error(f"create database db VGROUPS 4097")
        tdSql.error(f"create database db VGROUPS -1")
        tdSql.error(f"create database db VGROUPS 0")

        tdLog.info(f"====> SINGLE_STABLE [0 | 1, default: ]")
        tdSql.execute(f"create database db SINGLE_STABLE 1")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdSql.execute(f"drop database db")

        tdSql.execute(f"create database db SINGLE_STABLE 0")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdSql.execute(f"drop database db")
        tdSql.error(f"create database db SINGLE_STABLE 2")
        tdSql.error(f"create database db SINGLE_STABLE -1")
