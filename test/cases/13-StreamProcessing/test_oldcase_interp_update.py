import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseInterpDeleteUpdate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_delete_update(self):
        """Stream interp delete update

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpUpdate.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpUpdate1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpUpdate2.sim
        """

        self.streamInterpDelete0()
        # self.streamInterpDelete1()
        # self.streamInterpDelete2()
        # self.streamInterpUpdate()
        # self.streamInterpUpdate1()
        # self.streamInterpUpdate2()

    def streamInterpDelete0(self):
        tdLog.info(f"streamInterpDelete0")
        clusterComCheck.check_stream_status()

        tdSql.execute(f"alter local 'streamCoverage' '1';")

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(prev);"
        )

        clusterComCheck.check_stream_status()

        tdSql.execute(
            f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791214000,8,1,1,1.0) (1648791215000,10,1,1,1.0) (1648791215009,15,1,1,1.0) (1648791217001,4,1,1,1.0);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.queryCheckFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 15
            and tdSql.getData(4, 1) == 15,
        )

        tdLog.info(
            f"1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.queryCheckFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 8
            and tdSql.getData(3, 1) == 8
            and tdSql.getData(4, 1) == 8,
        )

        tdLog.info(
            f"2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.queryCheckFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 1
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 8
            and tdSql.getData(3, 1) == 8,
        )

        tdLog.info(
            f"3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.queryCheckFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 1,
        )

        tdLog.info(f"step2")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 1;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(
            f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(next);"
        )

        clusterComCheck.check_stream_status()

        tdSql.execute(
            f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791214000,8,1,1,1.0) (1648791215000,10,1,1,1.0) (1648791215009,15,1,1,1.0) (1648791217001,4,1,1,1.0);"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.queryCheckFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 10
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(
            f"1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.queryCheckFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 5
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 8
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4
            and tdSql.getData(4, 1) == 4,
        )

        tdLog.info(
            f"2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.queryCheckFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 4
            and tdSql.getData(0, 1) == 8
            and tdSql.getData(1, 1) == 4
            and tdSql.getData(2, 1) == 4
            and tdSql.getData(3, 1) == 4,
        )

        tdLog.info(
            f"3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;"
        )
        tdSql.execute(
            f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000"
        )

        tdLog.info(
            f"sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )
        tdSql.query(
            f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);"
        )

        tdLog.info(f"0 sql select * from streamt;")
        tdSql.queryCheckFunc(
            f"select * from streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 8,
        )


#     def streamInterpDelete1(self):
#         tdLog.info(f"streamInterpDelete1")
#         clusterComCheck.check_stream_status()

#         #system sh/stop_dnodes.sh
# #system sh/deploy.sh -n dnode1 -i 1
# #system sh/exec.sh -n dnode1 -s start
# sleep 50
#         tdSql.connect('root')

#         tdSql.execute(f"alter local 'streamCoverage' '1';")

#         tdLog.info(f'step1')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test vgroups 1;")
#         tdSql.execute(f"use test;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(NULL);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791214000,8,1,1,1.0) (1648791215000,10,1,1,1.0) (1648791215009,15,1,1,1.0) (1648791217001,4,1,1,1.0);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop0:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

# # row 0
#         tdSql.checkData(0, 1, None)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop0

#         tdSql.checkData(1, 1, 8)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop0

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop0

#         tdSql.checkData(3, 1, None)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop0

#         tdSql.checkData(4, 1, None)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop0

#         tdLog.info(f'1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop1:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

# # row 0
#         tdSql.checkData(0, 1, None)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop1

#         tdSql.checkData(1, 1, 8)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop1

#         tdSql.checkData(2, 1, None)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop1

#         tdSql.checkData(3, 1, None)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop1

#         tdSql.checkData(4, 1, None)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop1

#         tdLog.info(f'2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop2:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(4)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

# # row 0
#         tdSql.checkData(0, 1, 8)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop2

#         tdSql.checkData(1, 1, None)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop2

#         tdSql.checkData(2, 1, None)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop2

#         tdSql.checkData(3, 1, None)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop2

#         tdLog.info(f'3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop3:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(1)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

# # row 0
#         tdSql.checkData(0, 1, 8)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop3

#         tdLog.info(f'step2')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test2 vgroups 1;")
#         tdSql.execute(f"use test2;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(value,100,200,300,400);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791214000,8,1,1,1.0) (1648791215000,10,1,1,1.0) (1648791215009,15,1,1,1.0) (1648791217001,4,1,1,1.0);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value,100,200,300,400);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value,100,200,300,400);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop4:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop4

# # row 0
#         tdSql.checkData(0, 1, 100)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop4

#         tdSql.checkData(1, 1, 8)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop4

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop4

#         tdSql.checkData(3, 1, 100)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop4

#         tdSql.checkData(4, 1, 100)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop4

#         tdLog.info(f'1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value,100,200,300,400);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value,100,200,300,400);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop5:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop5

# # row 0
#         tdSql.checkData(0, 1, 100)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop5

#         tdSql.checkData(1, 1, 8)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop5

#         tdSql.checkData(2, 1, 100)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop5

#         tdSql.checkData(3, 1, 100)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop5

#         tdSql.checkData(4, 1, 100)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop5

#         tdLog.info(f'2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value,100,200,300,400);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value,100,200,300,400);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop6:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(4)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop6

# # row 0
#         tdSql.checkData(0, 1, 8)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop6

#         tdSql.checkData(1, 1, 100)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop6

#         tdSql.checkData(2, 1, 100)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop6

#         tdSql.checkData(3, 1, 100)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop6

#         tdLog.info(f'3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value,100,200,300,400);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value,100,200,300,400);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop7:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(1)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop7

# # row 0
#         tdSql.checkData(0, 1, 8)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop7

# #system sh/exec.sh -n dnode1 -s stop -x SIGINT


#     def streamInterpDelete2(self):
#         tdLog.info(f"streamInterpDelete2")
#         clusterComCheck.check_stream_status()

#         #system sh/stop_dnodes.sh
# #system sh/deploy.sh -n dnode1 -i 1
# #system sh/exec.sh -n dnode1 -s start
# sleep 50
#         tdSql.connect('root')

#         tdSql.execute(f"alter local 'streamCoverage' '1';")

#         tdLog.info(f'step1')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test vgroups 1;")
#         tdSql.execute(f"use test;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(linear);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791214000,8,1,1,1.0) (1648791215000,10,1,1,1.0) (1648791215009,15,1,1,1.0) (1648791217001,4,1,1,1.0);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linera);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop0:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

# # row 0
#         tdSql.checkData(0, 1, 4)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop0

#         tdSql.checkData(1, 1, 8)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop0

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop0

#         tdSql.checkData(3, 1, 9)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop0

#         tdSql.checkData(4, 1, 4)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop0

#         tdLog.info(f'1 sql delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791215000 and ts <= 1648791216000;")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop1:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

# # row 0
#         tdSql.checkData(0, 1, 4)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop1

#         tdSql.checkData(1, 1, 8)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop1

#         tdSql.checkData(2, 1, 6)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop1

#         tdSql.checkData(3, 1, 5)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop1

#         tdSql.checkData(4, 1, 4)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop1

#         tdLog.info(f'2 sql delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791212000 and ts <= 1648791213000;")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop2:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(4)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

# # row 0
#         tdSql.checkData(0, 1, 8)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop2

#         tdSql.checkData(1, 1, 6)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop2

#         tdSql.checkData(2, 1, 5)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop2

#         tdSql.checkData(3, 1, 4)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop2

#         tdLog.info(f'3 sql delete from t1 where ts >= 1648791217000 and ts <= 1648791218000;')
#         tdSql.execute(f"delete from t1 where ts >= 1648791217000 and ts <= 1648791218000")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop3:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 10 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(1)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

# # row 0
#         tdSql.checkData(0, 1, 8)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop3

# #system sh/exec.sh -n dnode1 -s stop -x SIGINT


#     def streamInterpUpdate(self):
#         tdLog.info(f"streamInterpUpdate")
#         clusterComCheck.check_stream_status()

# #system sh/stop_dnodes.sh
# #system sh/deploy.sh -n dnode1 -i 1
# #system sh/exec.sh -n dnode1 -s start
# sleep 50
#         tdSql.connect('root')

#         tdSql.execute(f"alter local 'streamCoverage' '1';")

#         tdLog.info(f'step1')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test vgroups 1;")
#         tdSql.execute(f"use test;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(prev);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791215000,10,1,1,1.0)  (1648791217001,4,1,1,1.0)")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop0:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

# # row 0
#         tdSql.checkData(0, 1, 1)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop0

#         tdSql.checkData(1, 1, 1)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop0

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop0

#         tdSql.checkData(3, 1, 10)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop0

#         tdSql.checkData(4, 1, 10)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop0

#         tdSql.execute(f"insert into t1 values(1648791212001,2,2,2,2.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop1:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

# # row 0
#         tdSql.checkData(0, 1, 2)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop1

#         tdSql.checkData(1, 1, 2)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop1

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop1

#         tdSql.checkData(3, 1, 10)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop1

#         tdSql.checkData(4, 1, 10)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop1

#         tdSql.execute(f"insert into t1 values(1648791215000,20,20,20,20.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop2:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

# # row 0
#         tdSql.checkData(0, 1, 2)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop2

#         tdSql.checkData(1, 1, 2)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop2

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop2

#         tdSql.checkData(3, 1, 20)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop2

#         tdSql.checkData(4, 1, 20)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop2

#         tdSql.execute(f"insert into t1 values(1648791217001,8,8,8,8.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop3:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

# # row 0
#         tdSql.checkData(0, 1, 2)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop3

#         tdSql.checkData(1, 1, 2)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop3

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop3

#         tdSql.checkData(3, 1, 20)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop3

#         tdSql.checkData(4, 1, 20)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop3

#         tdLog.info(f'step2')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test2 vgroups 1;")
#         tdSql.execute(f"use test2;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(next);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791215000,10,1,1,1.0)  (1648791217001,4,1,1,1.0)")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop4:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop4

# # row 0
#         tdSql.checkData(0, 1, 10)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop4

#         tdSql.checkData(1, 1, 10)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop4

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop4

#         tdSql.checkData(3, 1, 4)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop4

#         tdSql.checkData(4, 1, 4)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop4

#         tdSql.execute(f"insert into t1 values(1648791212001,2,2,2,2.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop5:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop5

# # row 0
#         tdSql.checkData(0, 1, 10)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop5

#         tdSql.checkData(1, 1, 10)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop5

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop5

#         tdSql.checkData(3, 1, 4)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop5

#         tdSql.checkData(4, 1, 4)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop5

#         tdSql.execute(f"insert into t1 values(1648791215000,20,20,20,20.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop6:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop6

# # row 0
#         tdSql.checkData(0, 1, 20)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop6

#         tdSql.checkData(1, 1, 20)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop6

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop6

#         tdSql.checkData(3, 1, 4)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop6

#         tdSql.checkData(4, 1, 4)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop6

#         tdSql.execute(f"insert into t1 values(1648791217001,8,8,8,8.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(next);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop7:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop7

# # row 0
#         tdSql.checkData(0, 1, 20)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop7

#         tdSql.checkData(1, 1, 20)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop7

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop7

#         tdSql.checkData(3, 1, 8)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop7

#         tdSql.checkData(4, 1, 8)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop7

# #system sh/exec.sh -n dnode1 -s stop -x SIGINT

#     def streamInterpUpdate1(self):
#         tdLog.info(f"streamInterpUpdate1")
#         clusterComCheck.check_stream_status()

#         #system sh/stop_dnodes.sh
# #system sh/deploy.sh -n dnode1 -i 1
# #system sh/exec.sh -n dnode1 -s start
# sleep 50
#         tdSql.connect('root')

#         tdSql.execute(f"alter local 'streamCoverage' '1';")

#         tdLog.info(f'step1')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test vgroups 1;")
#         tdSql.execute(f"use test;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(NULL);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791215000,10,1,1,1.0)  (1648791217001,4,1,1,1.0)")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop0:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

# # row 0
#         tdSql.checkData(0, 1, None)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop0

#         tdSql.checkData(1, 1, None)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop0

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop0

#         tdSql.checkData(3, 1, None)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop0

#         tdSql.checkData(4, 1, None)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop0

#         tdSql.execute(f"insert into t1 values(1648791212001,2,2,2,2.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop1:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

# # row 0
#         tdSql.checkData(0, 1, None)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop1

#         tdSql.checkData(1, 1, None)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop1

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop1

#         tdSql.checkData(3, 1, None)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop1

#         tdSql.checkData(4, 1, None)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop1

#         tdSql.execute(f"insert into t1 values(1648791215000,20,20,20,20.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(NULL);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop2:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

# # row 0
#         tdSql.checkData(0, 1, None)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop2

#         tdSql.checkData(1, 1, None)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop2

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop2

#         tdSql.checkData(3, 1, None)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop2

#         tdSql.checkData(4, 1, None)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop2

#         tdSql.execute(f"insert into t1 values(1648791217001,8,8,8,8.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(prev);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop3:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

# # row 0
#         tdSql.checkData(0, 1, None)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop3

#         tdSql.checkData(1, 1, None)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop3

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop3

#         tdSql.checkData(3, 1, None)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop3

#         tdSql.checkData(4, 1, None)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop3

#         tdLog.info(f'step2')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test2 vgroups 1;")
#         tdSql.execute(f"use test2;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams2 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(value, 100, 200, 300, 400);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791215000,10,1,1,1.0)  (1648791217001,4,1,1,1.0)")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop4:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop4

# # row 0
#         tdSql.checkData(0, 1, 100)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop4

#         tdSql.checkData(1, 1, 100)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop4

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop4

#         tdSql.checkData(3, 1, 100)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop4

#         tdSql.checkData(4, 1, 100)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop4

#         tdSql.execute(f"insert into t1 values(1648791212001,2,2,2,2.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop5:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop5

# # row 0
#         tdSql.checkData(0, 1, 100)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop5

#         tdSql.checkData(1, 1, 100)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop5

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop5

#         tdSql.checkData(3, 1, 100)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop5

#         tdSql.checkData(4, 1, 100)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop5

#         tdSql.execute(f"insert into t1 values(1648791215000,20,20,20,20.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop6:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop6

# # row 0
#         tdSql.checkData(0, 1, 100)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop6

#         tdSql.checkData(1, 1, 100)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop6

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop6

#         tdSql.checkData(3, 1, 100)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop6

#         tdSql.checkData(4, 1, 100)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop6

#         tdSql.execute(f"insert into t1 values(1648791217001,8,8,8,8.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(value, 100, 200, 300, 400);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop7:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop7

# # row 0
#         tdSql.checkData(0, 1, 100)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop7

#         tdSql.checkData(1, 1, 100)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop7

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop7

#         tdSql.checkData(3, 1, 100)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop7

#         tdSql.checkData(4, 1, 100)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop7

# #system sh/exec.sh -n dnode1 -s stop -x SIGINT


#     def streamInterpUpdate2(self):
#         tdLog.info(f"streamInterpUpdate2")
#         clusterComCheck.check_stream_status()

#         #system sh/stop_dnodes.sh
# #system sh/deploy.sh -n dnode1 -i 1
# #system sh/exec.sh -n dnode1 -s start
# sleep 50
#         tdSql.connect('root')

#         tdSql.execute(f"alter local 'streamCoverage' '1';")

#         tdLog.info(f'step1')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test vgroups 1;")
#         tdSql.execute(f"use test;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 into  streamt as select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 every(1s) fill(linear);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(1648791212001,1,1,1,1.0) (1648791215000,10,1,1,1.0)  (1648791217001,4,1,1,1.0)")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop0:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

# # row 0
#         tdSql.checkData(0, 1, 3)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop0

#         tdSql.checkData(1, 1, 6)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop0

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop0

#         tdSql.checkData(3, 1, 7)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop0

#         tdSql.checkData(4, 1, 4)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop0

#         tdSql.execute(f"insert into t1 values(1648791212001,2,2,2,2.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop1:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

# # row 0
#         tdSql.checkData(0, 1, 4)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop1

#         tdSql.checkData(1, 1, 7)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop1

#         tdSql.checkData(2, 1, 10)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop1

#         tdSql.checkData(3, 1, 7)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop1

#         tdSql.checkData(4, 1, 4)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop1

#         tdSql.execute(f"insert into t1 values(1648791215000,20,20,20,20.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop2:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

# # row 0
#         tdSql.checkData(0, 1, 7)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop2

#         tdSql.checkData(1, 1, 13)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop2

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop2

#         tdSql.checkData(3, 1, 12)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop2

#         tdSql.checkData(4, 1, 4)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop2

#         tdSql.execute(f"insert into t1 values(1648791217001,8,8,8,8.1);")

#         tdLog.info(f'sql select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);')
#         tdSql.query(f"select _irowts, interp(a), interp(b), interp(c), interp(d) from t1 range(1648791212000, 1648791217001) every(1s) fill(linear);")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0

# loop3:

# sleep 300

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'0 sql select * from streamt;')
#         tdSql.query(f"select * from streamt;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         tdSql.checkRows(5)
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

# # row 0
#         tdSql.checkData(0, 1, 7)
#           tdLog.info(f'======tdSql.getData(0,1)={tdSql.getData(0,1)}')
#   goto loop3

#         tdSql.checkData(1, 1, 13)
#           tdLog.info(f'======tdSql.getData(1,1)={tdSql.getData(1,1)}')
#   goto loop3

#         tdSql.checkData(2, 1, 20)
#           tdLog.info(f'======tdSql.getData(2,1)={tdSql.getData(2,1)}')
#   goto loop3

#         tdSql.checkData(3, 1, 14)
#           tdLog.info(f'======tdSql.getData(3,1)={tdSql.getData(3,1)}')
#   goto loop3

#         tdSql.checkData(4, 1, 8)
#           tdLog.info(f'======tdSql.getData(4,1)={tdSql.getData(4,1)}')
#   goto loop3

# #system sh/exec.sh -n dnode1 -s stop -x SIGINT
