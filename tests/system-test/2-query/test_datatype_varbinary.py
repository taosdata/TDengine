import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self._conn = conn

    def prepare_data(self, dbname="varbinary_db"):
        # create database
        tdSql.execute(f'''drop database if exists {dbname};''')
        tdSql.execute(f'''create database if not exists {dbname};''')
        tdSql.execute(f'''use {dbname};''')

    def test_varbinary_insert(self):
        """Verify the varbinary datatype can be inserted into TDengine successfully
        """
        try:
            # common table
            tdSql.execute(f'''create table t1(ts timestamp, c1 varbinary(64));''')
            tdSql.execute(f'''insert into t1 values(now, 'taosdata') (now+1s, '涛思数据') (now+2s, null) (now+3s, '');''')
            tdLog.info("Insert varbinary data into common table successfully")

            # super and child table
            tdSql.execute(f'''create stable st1 (ts timestamp, c1 varbinary(128), c2 int) tags(groupid int, marks varbinary(64));''')
            tdSql.execute(f'''create table ct1 using st1 tags(1, 'taosdata');''')
            tdSql.execute(f'''create table ct2 using st1 tags(2, 'test123');''')
            tdSql.execute(f'''insert into ct1 values(now, 'taosdata', 1) (now+1s, '涛思数据', 2) (now+2s, null, 3) (now+3s, '', 4) (now+4s, '~!@#$%^&*()_+?<>', 5);''')
            varbinary_128 = 'a' * 128
            varbinary_129 = 'a' * 129
            tdSql.execute(f'''insert into ct2 values(now, '{varbinary_128}', 1)''')
            tdLog.info("Insert varbinary data into child table successfully")

            tdSql.error(f'''insert into ct2 values(now, '{varbinary_129}', 2)''')
            tdLog.info("Test case test_varbinary_insert is finished successfully")
        except Exception as ex:
            raise Exception(f"test_varbinary_insert failed! Exception: {ex}")

    def test_varbinary_insert_boundary(self):
        """Verify the boundary length of varbinary datatype can be inserted into TDengine successfully
        """
        try:
            tdSql.execute(f'''create table t2(ts timestamp, c1 varbinary(65517));''')
            varbinary_65517 = 'a' * 65517
            res = tdSql.getResult(f'''desc t2;''')
            assert(res[1][2] == 65517)
            tdSql.execute(f'''insert into t2 values(now, '{varbinary_65517}');''')
            tdLog.info("Insert varbinary(65517) data into common table successfully")

            tdSql.execute(f'''create table st2(ts timestamp, c1 int) tags(marks varbinary(16382));''')
            varbinary_16382 = 'a' * 16382
            res = tdSql.getResult(f'''desc st2;''')
            assert(res[2][2] == 16382)
            tdSql.execute(f'''insert into ct2_1 using st2 tags('{varbinary_16382}') values(now, 1);''')
            tdLog.info("Insert varbinary(16384) data into tag of super table successfully")

            tdLog.info("Test case test_varbinary_insert_boundary is finished successfully")
        except Exception as ex:
            raise Exception(f"test_varbinary_insert_boundary failed! Exception: {ex}")

    def test_varbinary_insert_schemaless(self):
        """Verify the varbinary datatype can be inserted into TDengine with schemaless api successfully
        """
        try:
            lines = [
                '''st3,t1=b'taosdata' c1=2,c2=false,c3=b"varchar\\x98f46e" 1685586804002''',
                '''st3,t1=b'taosdata' c1=2,c2=false,c3=b"test" 1685586804003''',
                '''st3,t1=b'taosdata' c1=2,c2=false,c3=b"涛思数据" 1685586804004''',
                '''st3,t1=b'taosdata' c1=2,c2=true 1685586804005'''
            ]
            self._conn.schemaless_insert(lines, 1, 4)
            res = tdSql.getResult(f'''desc st3;''')
            assert(res[3][1] == 'VARBINARY')
            assert(res[4][1] == 'NCHAR')
            tdLog.info("Test case test_varbinary_insert_schemaless is finished successfully")
        except Exception as ex:
            raise Exception(f"test_varbinary_insert_schemaless failed! Exception: {ex}")

    def test_varbinary_update(self):
        """Verify the varbinary datatype column and tag cat be updated successfully
        """
        try:
            tdSql.execute(f'''alter table t1 modify column c1 varbinary(128);''')
            res = tdSql.getResult(f'''desc t1;''')
            assert(res[1][2] == 128)
            tdLog.info("Alter table t1 modify column c1 varbinary(128) successfully")

            tdSql.execute(f'''alter table st1 modify column c1 varbinary(256);''')
            res = tdSql.getResult(f'''desc st1;''')
            assert(res[1][2] == 256)
            tdLog.info("Alter super table st1 modify column c1 varbinary(256) successfully")
            
            tdSql.execute(f'''alter table st1 modify tag marks varbinary(128);''')
            res = tdSql.getResult(f'''desc st1;''')
            assert(res[4][2] == 128)
            tdLog.info("Alter super table st1 modify tag marks varbinary(128) successfully")
            tdSql.error(f'''alter table st1 modify column c1 varbinary(64);''')
            tdSql.error(f'''alter table st1 modify tag marks varbinary(64);''')
            tdLog.info("Test case test_varbinary_update is finished successfully")
        except Exception as ex:
            raise Exception(f"test_varbinary_update failed! Exception: {ex}")

    def test_varbinary_query_common(self):
        try:
            tdSql.execute(f'''create table t(ts timestamp, c1 int, c2 varbinary(32));''')
            tdSql.execute(f'''create table st(ts timestamp, c1 int, c2 varbinary(32), c3 varchar(32)) tags(groupid int, marks varbinary(64));''')
            tdSql.execute(f'''insert into t values(now, 1, "varchar\\xfea3bb0a") (now+1s, 2, "varchar\\xfea3bb0b") \
                (now+2s, 3, "varchar\\xfea3bb0c") (now+3s, 4, "varchar\\xfea3bb0d") (now+4s, 5, "varchar\\xfea3bb0e") \
                (now+5s, 6, null) (now+6s, 7, '') (now+7s, 8, "varchar\\xfea3bb0f") (now+8s, 9, "varchar\\xfea3bb0f") \
                 (now+9s, 10, "涛思数据") (now+10s, 11, "taosdata");''')
            tdSql.execute(f'''insert into ct1 using st tags(1, "varchar\\xfafbfc1a") values(now, 1, "varchar\\xfea3bb0a", "taosdata") \
                (now+1s, 2, "varchar\\xfea3bb0b", "tdengine") (now+2s, 3, "varchar\\xfea3bb0c", "!@$#%^") (now+3s, 4, "varchar\\xfea3bb0d", "  test  ") \
                (now+4s, 5, "varchar\\xfea3bb0e", "涛思数据");''')
            tdSql.execute(f'''insert into ct2 using st tags(2, "varchar\\xfafbfc1b") values(now, 6, null, '???~') \
                (now+1s, 7, '', '')  (now+2s, 8, "varchar\\xfea3bb0f", '') (now+3s, 9, "varchar\\xfea3bb0f", 'null') \
                (now+4s, 10, "涛思数据", 'testdata') (now+5s, 11, "taosdata", 'none');''')

            # entire columns query
            res = tdSql.getResult(f'''select * from t;''')
            assert(len(res) == 11)
            res = tdSql.getResult(f'''select * from st;''')
            assert(len(res) == 11)

            # column query
            res = tdSql.getResult(f'''select c2 from t;''')
            assert(len(res) == 11)
            res = tdSql.getResult(f'''select c2 from st;''')
            assert(len(res) == 11)

            # column query with condition
            res = tdSql.getResult(f'''select * from t where c2 >= "varchar\\xfea3bb0a";''')
            assert(len(res) == 8)
            res = tdSql.getResult(f'''select * from st where c2 >= "varchar\\xfea3bb0a";''')
            assert(len(res) == 8)

            res = tdSql.getResult(f'''select * from t where c2 <= "varchar\\xfea3bb0a";''')
            assert(len(res) == 3)
            res = tdSql.getResult(f'''select * from st where c2 <= "varchar\\xfea3bb0a";''')
            assert(len(res) == 3)

            res = tdSql.getResult(f'''select * from t where c2 between "varchar\\xfea3bb0a" and "varchar\\xfea3bb0c";''')
            assert(len(res) == 3)
            res = tdSql.getResult(f'''select * from st where c2 between "varchar\\xfea3bb0a" and "varchar\\xfea3bb0c";''')
            assert(len(res) == 3)

            res = tdSql.getResult(f'''select * from t where c2 is null;''')
            assert(len(res) == 1)
            res = tdSql.getResult(f'''select * from st where c2 is null;''')
            assert(len(res) == 1)

            res = tdSql.getResult(f'''select * from t where c2 is not null;''')
            assert(len(res) == 10)
            res = tdSql.getResult(f'''select * from st where c2 is not null;''')
            assert(len(res) == 10)

            res = tdSql.getResult(f'''select * from t where c2 in ("varchar\\xfea3bb0a", "varchar\\xfea3bb0f");''')
            assert(len(res) == 3)
            res = tdSql.getResult(f'''select * from st where c2 in ("varchar\\xfea3bb0a", "varchar\\xfea3bb0f");''')
            assert(len(res) == 3)

            res = tdSql.getResult(f'''select * from t where c2 not in ("varchar\\xfea3bb0a", "varchar\\xfea3bb0f");''')
            assert(len(res) == 7)
            res = tdSql.getResult(f'''select * from st where c2 not in ("varchar\\xfea3bb0a", "varchar\\xfea3bb0f");''')
            assert(len(res) == 7)

            res = tdSql.getResult(f'''select * from t order by c2;''')
            tdLog.info(res)
            tdLog.info(res[0][2])
            assert(res[0][2] is None)
            res = tdSql.getResult(f'''select * from st order by c2;''')
            assert(res[0][2] is None)

            res = tdSql.getResult(f'''select count(*) from t group by c2;''')
            assert(len(res) == 10)
            res = tdSql.getResult(f'''select count(*) from st group by c2;''')
            assert(len(res) == 10)

            # don't support math operator, bit operator and [NOT] LIKE/MATCH/NMATCH/->/CONTAINS
            tdSql.error(f'''select c2 + 'varchar\\x02' from t;''')
            tdSql.error(f'''select c2 - 'varchar\\x02' from st;''')
            tdSql.error(f'''select c2 * 'varchar\\x02' from t;''')
            tdSql.error(f'''select c2 / 'varchar\\x02' from st;''')
            tdSql.error(f'''select c2 % 'varchar\\x02' from t;''')
            tdSql.error(f'''select c2 & 'varchar\\x02' from st;''')
            tdSql.error(f'''select c2 | 'varchar\\x02' from t;''')
            tdSql.error(f'''select * from t where c2 like "varchar\\xfea3bb0a";''')
            tdSql.error(f'''select * from st where c2 not like "varchar\\xfea3bb0a";''')
            tdSql.error(f'''select * from t where c2 contains "varchar\\xfea3bb0a";''')
            tdSql.error(f'''select * from t where c2 match "%varchar\\xfea3%";''')
            tdSql.error(f'''select * from st where c2 nmatch "%varchar\\xfea%";''')

            tdLog.info("Test case test_varbinary_query_common is finished successfully")
        except Exception as ex:
            raise Exception(f"test_varbinary_query_common failed! Exception: {ex}")

    def test_varbinary_query_function(self):
        try:
            tdSql.query(f'''select first(c2) from st;''')
            tdSql.query(f'''select last(c2) from st;''')
            tdSql.query(f'''select last_row(c2) from st;''')
            res = tdSql.getResult(f'''select count(c2) from st;''')
            assert(res[0][0] == 10)
            tdSql.query(f'''select hyperloglog(c2) from st;''')
            res = tdSql.getResult(f'''select sample(c2, 2) from st;''')
            assert(len(res) == 2)
            res = tdSql.getResult(f'''select tail(c2, 6) from st;''')
            assert(len(res) == 6)
            res = tdSql.getResult(f'''select mode(c2) from st;''')
            assert(len(res) == 1)
            res = tdSql.getResult(f'''select cast(c3 as varbinary(32)) from st;''')
            assert(len(res) == 11)

            tdSql.error(f'''select min(c2) from st;''')
            tdSql.error(f'''select max(c2) from st;''')
            tdSql.error(f'''select abs(c2) from st;''')
            tdSql.error(f'''select ltrim(c2) from st;''')
            tdLog.info("Test case test_varbinary_query_function is finished successfully")
        except Exception as ex:
            raise Exception(f"test_varbinary_query_function failed! Exception: {ex}")

    def test_varbinary_query_join(self):
        try:
            res = tdSql.getResult(f'''select * from st as st1 join st as st2 on st1.ts = st2.ts and st1.c2 <= st2.c2;''')
            assert(len(res) == 10)
            res = tdSql.getResult(f'''select count(st1.c2) from st as st1 join st as st2 on st1.ts = st2.ts;''')
            assert(res[0][0] == 10)

            tdSql.error(f'''select sum(st1.c2) from st as st1 join st as st2 on st1.ts = st2.ts;''')
            tdLog.info("Test case test_varbinary_query_join is finished successfully")
        except Exception as ex:
            raise Exception(f"test_varbinary_query_join failed! Exception: {ex}")

    def test_varbinary_query_show_cmd(self):
        try:
            res = tdSql.getResult(f'''show create table t;''')
            assert('VARBINARY(32)' in res[0][1])
            
            res = tdSql.getResult(f'''show create table st;''')
            assert('VARBINARY(32)' in res[0][1] and 'VARBINARY(64)' in res[0][1])
            tdLog.info("Test case test_varbinary_query_show_cmd is finished successfully")
        except Exception as ex:
            raise Exception(f"test_varbinary_query_show_cmd failed! Exception: {ex}")

    def run(self):
        self.prepare_data()
        self.test_varbinary_insert()
        self.test_varbinary_insert_boundary()
        self.test_varbinary_insert_schemaless()
        self.test_varbinary_update()
        self.prepare_data()
        self.test_varbinary_query_common()
        self.test_varbinary_query_function()
        self.test_varbinary_query_join()
        self.test_varbinary_query_show_cmd()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
