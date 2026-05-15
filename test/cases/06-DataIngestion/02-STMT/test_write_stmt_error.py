# encoding:UTF-8
from taos import *

from ctypes import *
import taos

from new_test_framework.utils import tdLog


class TestStmtError:
    # def __init__(self):
    #     self.err_case = 0
    #     self.curret_case = 0

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql), logSql)
        cls.err_case = 0
        cls.curret_case = 0

    def get_connect(self):
        # type: () -> taos.TaosConnection
        return taos.connect()

    def check_stmt_insert(self,conn):
        # type: (TaosConnection) -> None

        dbname = "pytest_taos_stmt"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(65059), nn nchar(100), tt timestamp)",
            )
            conn.load_table_info("log")


            stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            params = taos.new_bind_params(16)
            params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
            params[1].bool(True)
            params[2].tinyint(None)
            params[3].tinyint(2)
            params[4].smallint(3)
            params[5].int(4)
            params[6].bigint(5)
            params[7].tinyint_unsigned(6)
            params[8].smallint_unsigned(7)
            params[9].int_unsigned(8)
            params[10].bigint_unsigned(9)
            params[11].float(10.1)
            params[12].double(10.11)
            binaryStr = '123456789'
            for i in range(1301):
                binaryStr += "1234567890abcdefghij1234567890abcdefghij12345hello"
            params[13].binary(binaryStr)
            params[14].nchar("stmt")
            params[15].timestamp(1626861392589, PrecisionEnum.Milliseconds)

            stmt.bind_param(params)
            stmt.execute()

            assert stmt.affected_rows == 1
            stmt.close()

            querystmt=conn.statement("select ?, bo, nil, ti, si, ii,bi, tu, su, iu, bu, ff, dd, bb, nn, tt from log")
            queryparam=new_bind_params(1)
            print(type(queryparam))
            queryparam[0].binary("ts")
            querystmt.bind_param(queryparam)
            querystmt.execute()
            result=querystmt.use_result()

            row=result.fetch_all()
            print(row)

            assert row[0][1] == True
            assert row[0][2] == None
            for i in range(3, 10):
                assert row[0][i] == i - 1
            #float == may not work as expected
            # assert row[0][11] == c_float(10.1)
            assert row[0][12] == 10.11
            assert row[0][13][65054:] == "hello"
            assert row[0][14] == "stmt"

            conn.execute("drop database if exists %s" % dbname)
            conn.close()

        except Exception as err:
            conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def check_stmt_insert_error(self,conn):
        # type: (TaosConnection) -> None

        dbname = "pytest_taos_stmt_error"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , error_data int )",
            )
            conn.load_table_info("log")


            stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1000)")
            params = new_bind_params(16)
            params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
            params[1].bool(True)
            params[2].tinyint(None)
            params[3].tinyint(2)
            params[4].smallint(3)
            params[5].int(4)
            params[6].bigint(5)
            params[7].tinyint_unsigned(6)
            params[8].smallint_unsigned(7)
            params[9].int_unsigned(8)
            params[10].bigint_unsigned(9)
            params[11].float(10.1)
            params[12].double(10.11)
            params[13].binary("hello")
            params[14].nchar("stmt")
            params[15].timestamp(1626861392589, PrecisionEnum.Milliseconds)

            stmt.bind_param(params)
            stmt.execute()

            conn.close()

        except Exception as err:
            conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def check_stmt_insert_vtb_error(self,conn):
        # type: (TaosConnection) -> None

        dbname = "pytest_taos_stmt_vtb_error"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(65059), nn nchar(100), tt timestamp)",
            )

            conn.execute(
                "create vtable if not exists log_v(ts timestamp, bo bool from pytest_taos_stmt_vtb_error.log.bo, "
                "nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(65059), nn nchar(100), tt timestamp)",
            )
            conn.load_table_info("log_v")


            stmt = conn.statement("insert into log_v values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            params = new_bind_params(16)
            params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
            params[1].bool(True)
            params[2].tinyint(None)
            params[3].tinyint(2)
            params[4].smallint(3)
            params[5].int(4)
            params[6].bigint(5)
            params[7].tinyint_unsigned(6)
            params[8].smallint_unsigned(7)
            params[9].int_unsigned(8)
            params[10].bigint_unsigned(9)
            params[11].float(10.1)
            params[12].double(10.11)
            binaryStr = '123456789'
            for i in range(1301):
                binaryStr += "1234567890abcdefghij1234567890abcdefghij12345hello"
            params[13].binary(binaryStr)
            params[14].nchar("stmt")
            params[15].timestamp(1626861392589, PrecisionEnum.Milliseconds)

            stmt.bind_param(params)
            stmt.execute()

            assert stmt.affected_rows == 1
            stmt.close()

            querystmt=conn.statement("select ?, bo, nil, ti, si, ii,bi, tu, su, iu, bu, ff, dd, bb, nn, tt from log")
            queryparam=new_bind_params(1)
            print(type(queryparam))
            queryparam[0].binary("ts")
            querystmt.bind_param(queryparam)
            querystmt.execute()
            result=querystmt.use_result()

            row=result.fetch_all()
            print(row)

            assert row[0][1] == True
            assert row[0][2] == None
            for i in range(3, 10):
                assert row[0][i] == i - 1
            #float == may not work as expected
            # assert row[0][11] == c_float(10.1)
            assert row[0][12] == 10.11
            assert row[0][13][65054:] == "hello"
            assert row[0][14] == "stmt"

            conn.execute("drop database if exists %s" % dbname)
            conn.close()

        except Exception as err:
            conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def check_stmt_insert_vstb_error(self,conn):

        dbname = "pytest_taos_stmt_vstb_error"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.execute("alter database %s keep 36500" % dbname)
            conn.select_db(dbname)

            conn.execute("create stable STB_v(ts timestamp, n int) tags(b int) virtual 1")

            stmt = conn.statement("insert into ? using STB_v tags(?) values(?, ?)")
            params = new_bind_params(1)
            params[0].int(4);
            stmt.set_tbname_tags("ct", params);

            multi_params = new_multi_binds(2);
            multi_params[0].timestamp([9223372036854775808])
            multi_params[1].int([123])
            stmt.bind_param_batch(multi_params)

            stmt.execute()
            result = stmt.use_result()

            result.close()
            stmt.close()
            conn.close()

        except Exception as err:
            conn.close()
            raise err

    def check_stmt_insert_error_null_timestamp(self,conn):

        dbname = "pytest_taos_stmt_error_null_ts"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.execute("alter database %s keep 36500" % dbname)
            conn.select_db(dbname)

            conn.execute("create stable STB(ts timestamp, n int) tags(b int)")

            stmt = conn.statement("insert into ? using STB tags(?) values(?, ?)")
            params = new_bind_params(1)
            params[0].int(4);
            stmt.set_tbname_tags("ct", params);

            multi_params = new_multi_binds(2);
            multi_params[0].timestamp([9223372036854775808])
            multi_params[1].int([123])
            stmt.bind_param_batch(multi_params)

            stmt.execute()
            result = stmt.use_result()

            result.close()
            stmt.close()
            conn.close()

        except Exception as err:
            conn.close()
            raise err

    def check_stmt_nornmal_value_error(self, conn):
        # type: (TaosConnection) -> None
        dbname = "pytest_taos_stmt_error"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , error_data int )",
            )
            conn.load_table_info("log")


            stmt = conn.statement("insert into log values(NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            params = new_bind_params(16)
            params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
            params[1].bool(True)
            params[2].tinyint(None)
            params[3].tinyint(2)
            params[4].smallint(3)
            params[5].int(4)
            params[6].bigint(5)
            params[7].tinyint_unsigned(6)
            params[8].smallint_unsigned(7)
            params[9].int_unsigned(8)
            params[10].bigint_unsigned(9)
            params[11].float(10.1)
            params[12].double(10.11)
            params[13].binary("hello")
            params[14].nchar("stmt")
            params[15].timestamp(1626861392589, PrecisionEnum.Milliseconds)

            stmt.bind_param(params)
            stmt.execute()

            conn.close()

        except Exception as err:
            conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def check_stmt_select_no_param(self, conn):
        """Execute a zero-placeholder STMT SELECT and propagate any exception.

        Sets up a minimal table, prepares a SELECT without '?' via STMT, then
        calls execute() without bind_param().  The caller decides whether
        success or failure is the expected outcome.
        """
        dbname = "pytest_taos_stmt_no_param"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)
            conn.execute("create table t1 (ts timestamp, v int)")
            conn.execute("insert into t1 values(1626861392589, 42)")

            stmt = conn.statement("select * from t1")
            try:
                stmt.execute()
            finally:
                stmt.close()

            conn.execute("drop database if exists %s" % dbname)
            conn.close()
        except Exception as err:
            conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def test_stmt_select_no_param(self):
        """STMT SELECT without '?' placeholders returns Stmt API usage error

        A zero-placeholder SELECT prepared via STMT then executed directly
        (no bind_param call) is not supported on this branch and must fail
        with [0x022a]: Stmt API usage error.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-4-27 Added to document baseline behavior for 0-param STMT SELECT
        """
        expected = "[0x022a]: Stmt API usage error"
        try:
            self.check_stmt_select_no_param(self.get_connect())
            # stmt.execute() unexpectedly succeeded — fail the test explicitly.
            raise AssertionError("expected '%s' but stmt.execute() succeeded" % expected)
        except AssertionError:
            raise
        except Exception as error:
            if expected in str(error):
                tdLog.info("0-param STMT SELECT correctly returns: %s" % error)
            else:
                raise AssertionError(
                    "unexpected error for 0-param STMT SELECT: %s" % str(error)
                ) from error

    def test_stmt_error(self):
        """STMT error

        1. Write data with STMT
        2. Query data with STMT
        3. Write to virtual table with STMT expect error
        4. Write to super table with null timestamp expect error
        5. Write to super table with normal value expect error

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_stmt_error.py
        """
        self.check_stmt_insert(self.get_connect())

        try:
            self.check_stmt_insert_error_null_timestamp(self.get_connect())
            raise AssertionError("expected '[0x060b]: Timestamp data out of range' but no error occurred")
        except AssertionError:
            raise
        except Exception as error:
            if "[0x060b]: Timestamp data out of range" in str(error):
                tdLog.info("stmt error correctly occurred for NULL timestamp: %s" % error)
            else:
                raise AssertionError("unexpected error for NULL timestamp STMT insert: %s" % str(error)) from error

        try:
            self.check_stmt_insert_vtb_error(self.get_connect())
            raise AssertionError("expected '[0x6205]: Virtual table not support' but no error occurred")
        except AssertionError:
            raise
        except Exception as error:
            if "[0x6205]: Virtual table not support in STMT query and STMT insert" in str(error):
                tdLog.info("stmt error correctly occurred for virtual table insert: %s" % error)
            else:
                raise AssertionError("unexpected error for virtual table STMT insert: %s" % str(error)) from error

        try:
            self.check_stmt_insert_vstb_error(self.get_connect())
            raise AssertionError("expected '[0x6205]: Virtual table not support' but no error occurred")
        except AssertionError:
            raise
        except Exception as error:
            if "[0x6205]: Virtual table not support in STMT query and STMT insert" in str(error):
                tdLog.info("stmt error correctly occurred for virtual super table insert: %s" % error)
            else:
                raise AssertionError("unexpected error for virtual super table STMT insert: %s" % str(error)) from error
        

