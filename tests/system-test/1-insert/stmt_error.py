# encoding:UTF-8
from taos import *

from ctypes import *
from datetime import datetime
import taos

import taos
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:
    def __init__(self):
        self.err_case = 0
        self.curret_case = 0

    def caseDescription(self):

        '''
        case1 <wenzhouwww>: [TD-11899] : this is an test case for check stmt error use .
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def conn(self):
    # type: () -> taos.TaosConnection
        return connect()

    def test_stmt_insert(self,conn):
        # type: (TaosConnection) -> None

        dbname = "pytest_taos_stmt"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
            )
            conn.load_table_info("log")
            

            stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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

            result = stmt.use_result()
            assert result.affected_rows == 1
            result.close()
            stmt.close()

            stmt = conn.statement("select * from log")
            stmt.execute()
            result = stmt.use_result()
            row  = result.next()
            print(row)
            assert row[2] == None
            for i in range(3, 11):
                assert row[i] == i - 1
            #float == may not work as expected
            # assert row[10] == c_float(10.1)
            assert row[12] == 10.11
            assert row[13] == "hello"
            assert row[14] == "stmt"

            conn.execute("drop database if exists %s" % dbname)
            conn.close()

        except Exception as err:
            conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def test_stmt_insert_error(self,conn):
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

            result = stmt.use_result()
            assert result.affected_rows == 1
            result.close()
            stmt.close()

            stmt = conn.statement("select * from log")
            stmt.execute()
            result = stmt.use_result()
            row  = result.next()
            print(row)
            assert row[2] == None
            for i in range(3, 11):
                assert row[i] == i - 1
            #float == may not work as expected
            # assert row[10] == c_float(10.1)
            assert row[12] == 10.11
            assert row[13] == "hello"
            assert row[14] == "stmt"

            conn.execute("drop database if exists %s" % dbname)
            conn.close()

        except Exception as err:
            conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def test_stmt_insert_error_null_timestamp(self,conn):
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

            stmt = conn.statement("select * from STB")
            stmt.execute()
            result = stmt.use_result()
            print(result.affected_rows)
            row  = result.next()
            print(row)

            result.close()
            stmt.close()
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()

        except Exception as err:
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err
    
    def run(self):
        
        self.test_stmt_insert(self.conn())
        try:
            self.test_stmt_insert_error(self.conn())
        except Exception as error :
            
            if str(error)=='[0x0200]: invalid operation: only ? allowed in values':
                tdLog.info('=========stmt error occured  for bind part column ==============')
            else:
                tdLog.exit("expect error not occured")

        try:    
            self.test_stmt_insert_error_null_timestamp(self.conn())
            tdLog.exit("expect error not occured - 1")
        except Exception as error :
            if str(error)=='[0x0200]: invalid operation: bind column type mismatch or invalid':
                tdLog.exit('=========stmt error occured  for bind part column(NULL Timestamp) ==============')
            else:
                tdLog.exit("expect error not occured - 2")
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
