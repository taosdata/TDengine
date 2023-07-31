import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.dbname = "db"

    def __cast_to_bigint(self, col_name, tbname):
        __sql = f"select cast({col_name} as bigint), {col_name} from {tbname}"
        tdSql.query(sql=__sql)
        data_tb_col = [result[1] for result in tdSql.queryResult]
        for i in range(tdSql.queryRows):
            tdSql.checkData( i, 0, None ) if data_tb_col[i] is None else tdSql.checkData( i, 0, int(data_tb_col[i]) )

    def __range_to_bigint(self,cols,tables):
        for col in cols:
            for table in tables:
                self.__cast_to_bigint(col_name=col, tbname=table)

    def __cast_to_timestamp(self, col_name, tbname):
        __sql = f"select cast({col_name} as timestamp), {col_name} from {tbname}"
        tdSql.query(sql=__sql)
        data_tb_col = [result[1] for result in tdSql.queryResult]
        for i in range(tdSql.queryRows):
            if data_tb_col[i] is None:
                tdSql.checkData( i, 0 , None )
            if col_name not in ["c2", "double"] or tbname != f"{self.dbname}.t1" or i != 10:
                date_init_stamp = datetime.datetime.fromtimestamp(data_tb_col[i]/1000)
                tdSql.checkData( i, 0, date_init_stamp)

    def __range_to_timestamp(self, cols, tables):
        for col in cols:
            for table in tables:
                self.__cast_to_timestamp(col_name=col, tbname=table)

    def __test_bigint(self):
        __table_list = [f"{self.dbname}.ct1", f"{self.dbname}.ct4", f"{self.dbname}.t1"]
        __col_list = ["c1","c2","c3","c4","c5","c6","c7","c10","c1+c2"]
        self.__range_to_bigint(cols=__col_list, tables=__table_list)

    def __test_timestamp(self):
        __table_list = [f"{self.dbname}.ct1", f"{self.dbname}.ct4", f"{self.dbname}.t1"]
        __col_list = ["c1","c2","c3","c4","c5","c6","c7","c1+c2"]
        self.__range_to_timestamp(cols=__col_list, tables=__table_list)

    def all_test(self):
        _datetime_epoch = datetime.datetime.fromtimestamp(0)
        tdSql.query(f"select c1  from {self.dbname}.ct4")
        data_ct4_c1 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c1  from {self.dbname}.t1")
        data_t1_c1 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdLog.printNoPrefix("==========step2: cast int to bigint, expect no changes")

        tdSql.query(f"select cast(c1 as bigint) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c1)):
            tdSql.checkData( i, 0, data_ct4_c1[i])
        tdSql.query(f"select cast(c1 as bigint) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c1)):
            tdSql.checkData( i, 0, data_t1_c1[i])

        tdLog.printNoPrefix("==========step5: cast int to binary, expect changes to str(int) ")

        #tdSql.query(f"select cast(c1 as binary(32)) as b from {self.dbname}.ct4")
        #for i in range(len(data_ct4_c1)):
        #    tdSql.checkData( i, 0, str(data_ct4_c1[i]) )
        tdSql.query(f"select cast(c1 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c1)):
            tdSql.checkData( i, 0, str(data_t1_c1[i]) )

        tdLog.printNoPrefix("==========step6: cast int to nchar, expect changes to str(int) ")

        tdSql.query(f"select cast(c1 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c1)):
            tdSql.checkData( i, 0, str(data_ct4_c1[i]) )
        tdSql.query(f"select cast(c1 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c1)):
            tdSql.checkData( i, 0, str(data_t1_c1[i]) )

        tdLog.printNoPrefix("==========step7: cast int to timestamp, expect changes to timestamp ")

        tdSql.query(f"select cast(c1 as timestamp) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c1)):
            if data_ct4_c1[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_ct4_c1[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)

        tdSql.query(f"select cast(c1 as timestamp) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c1)):
            if data_t1_c1[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c1[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)


        tdLog.printNoPrefix("==========step8: cast bigint to bigint, expect no changes")
        tdSql.query(f"select c2  from {self.dbname}.ct4")
        data_ct4_c2 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c2  from {self.dbname}.t1")
        data_t1_c2 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        tdSql.query(f"select cast(c2 as bigint) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c2)):
            tdSql.checkData( i, 0, data_ct4_c2[i])
        tdSql.query(f"select cast(c2 as bigint) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c2)):
            tdSql.checkData( i, 0, data_t1_c2[i])


        tdLog.printNoPrefix("==========step9: cast bigint to binary, expect changes to str(int) ")

        tdSql.query(f"select cast(c2 as binary(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c2)):
            tdSql.checkData( i, 0, str(data_ct4_c2[i]) )
        tdSql.query(f"select cast(c2 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c2)):
            tdSql.checkData( i, 0, str(data_t1_c2[i]) )

        tdLog.printNoPrefix("==========step10: cast bigint to nchar, expect changes to str(int) ")

        tdSql.query(f"select cast(c2 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c2)):
            tdSql.checkData( i, 0, str(data_ct4_c2[i]) )
        tdSql.query(f"select cast(c2 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c2)):
            tdSql.checkData( i, 0, str(data_t1_c2[i]) )

        tdLog.printNoPrefix("==========step11: cast bigint to timestamp, expect changes to timestamp ")

        tdSql.query(f"select cast(c2 as timestamp) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c2)):
            if data_ct4_c2[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_ct4_c2[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)


        tdSql.query(f"select cast(c2 as timestamp) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c2)):
            if data_t1_c2[i] is None:
                tdSql.checkData( i, 0 , None )
            elif i == 10:
                continue
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c2[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)


        tdLog.printNoPrefix("==========step12: cast smallint to bigint, expect no changes")
        tdSql.query(f"select c3  from {self.dbname}.ct4")
        data_ct4_c3 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c3  from {self.dbname}.t1")
        data_t1_c3 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        tdSql.query(f"select cast(c3 as bigint) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c3)):
            tdSql.checkData( i, 0, data_ct4_c3[i])
        tdSql.query(f"select cast(c3 as bigint) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c3)):
            tdSql.checkData( i, 0, data_t1_c3[i])


        tdLog.printNoPrefix("==========step13: cast smallint to binary, expect changes to str(int) ")

        tdSql.query(f"select cast(c3 as binary(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c3)):
            tdSql.checkData( i, 0, str(data_ct4_c3[i]) )
        tdSql.query(f"select cast(c3 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c3)):
            tdSql.checkData( i, 0, str(data_t1_c3[i]) )

        tdLog.printNoPrefix("==========step14: cast smallint to nchar, expect changes to str(int) ")

        tdSql.query(f"select cast(c3 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c3)):
            tdSql.checkData( i, 0, str(data_ct4_c3[i]) )
        tdSql.query(f"select cast(c3 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c3)):
            tdSql.checkData( i, 0, str(data_t1_c3[i]) )

        tdLog.printNoPrefix("==========step15: cast smallint to timestamp, expect changes to timestamp ")

        tdSql.query(f"select cast(c3 as timestamp) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c3)):
            if data_ct4_c3[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_ct4_c3[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)

        tdSql.query(f"select cast(c3 as timestamp) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c3)):
            if data_t1_c3[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c3[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)


        tdLog.printNoPrefix("==========step16: cast tinyint to bigint, expect no changes")
        tdSql.query(f"select c4  from {self.dbname}.ct4")
        data_ct4_c4 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c4  from {self.dbname}.t1")
        data_t1_c4 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        tdSql.query(f"select cast(c4 as bigint) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c4)):
            tdSql.checkData( i, 0, data_ct4_c4[i])
        tdSql.query(f"select cast(c4 as bigint) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c4)):
            tdSql.checkData( i, 0, data_t1_c4[i])


        tdLog.printNoPrefix("==========step17: cast tinyint to binary, expect changes to str(int) ")

        tdSql.query(f"select cast(c4 as binary(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c4)):
            tdSql.checkData( i, 0, str(data_ct4_c4[i]) )
        tdSql.query(f"select cast(c4 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c4)):
            tdSql.checkData( i, 0, str(data_t1_c4[i]) )

        tdLog.printNoPrefix("==========step18: cast tinyint to nchar, expect changes to str(int) ")

        tdSql.query(f"select cast(c4 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c4)):
            tdSql.checkData( i, 0, str(data_ct4_c4[i]) )
        tdSql.query(f"select cast(c4 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c4)):
            tdSql.checkData( i, 0, str(data_t1_c4[i]) )

        tdLog.printNoPrefix("==========step19: cast tinyint to timestamp, expect changes to timestamp ")

        tdSql.query(f"select cast(c4 as timestamp) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c4)):
            if data_ct4_c4[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_ct4_c4[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)

        tdSql.query(f"select cast(c4 as timestamp) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c4)):
            if data_t1_c4[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c4[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)


        tdLog.printNoPrefix("==========step20: cast float to bigint, expect no changes")
        tdSql.query(f"select c5  from {self.dbname}.ct4")
        data_ct4_c5 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c5  from {self.dbname}.t1")
        data_t1_c5 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        tdSql.query(f"select cast(c5 as bigint) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c5)):
            tdSql.checkData( i, 0, None ) if data_ct4_c5[i] is None else tdSql.checkData( i, 0, int(data_ct4_c5[i]) )
        tdSql.query(f"select cast(c5 as bigint) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            tdSql.checkData( i, 0, None ) if data_t1_c5[i] is None else tdSql.checkData( i, 0, int(data_t1_c5[i]) )

        tdLog.printNoPrefix("==========step21: cast float to binary, expect changes to str(int) ")
        tdSql.query(f"select cast(c5 as binary(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c5)):
            tdSql.checkData( i, 0, str(data_ct4_c5[i]) )  if data_ct4_c5[i] is None else  tdSql.checkData( i, 0, f'{data_ct4_c5[i]:.6f}' )
        tdSql.query(f"select cast(c5 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            tdSql.checkData( i, 0, str(data_t1_c5[i]) )  if data_t1_c5[i] is None else tdSql.checkData( i, 0, f'{data_t1_c5[i]:.6f}' )

        tdLog.printNoPrefix("==========step22: cast float to nchar, expect changes to str(int) ")
        tdSql.query(f"select cast(c5 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c5)):
            tdSql.checkData( i, 0, None )  if data_ct4_c5[i] is None else  tdSql.checkData( i, 0, f'{data_ct4_c5[i]:.6f}' )
        tdSql.query(f"select cast(c5 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            tdSql.checkData( i, 0, None )  if data_t1_c5[i] is None else tdSql.checkData( i, 0, f'{data_t1_c5[i]:.6f}' )

        tdLog.printNoPrefix("==========step23: cast float to timestamp, expect changes to timestamp ")
        tdSql.query(f"select cast(c5 as timestamp) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c5)):
            if data_ct4_c5[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_ct4_c5[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)
        tdSql.query(f"select cast(c5 as timestamp) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            if data_t1_c5[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c5[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)

        tdLog.printNoPrefix("==========step24: cast double to bigint, expect no changes")
        tdSql.query(f"select c6  from {self.dbname}.ct4")
        data_ct4_c6 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c6  from {self.dbname}.t1")
        data_t1_c6 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        tdSql.query(f"select cast(c6 as bigint) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c6)):
            tdSql.checkData( i, 0, None ) if data_ct4_c6[i] is None else tdSql.checkData( i, 0, int(data_ct4_c6[i]) )
        tdSql.query(f"select cast(c6 as bigint) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c6)):
            if data_t1_c6[i] is None:
                tdSql.checkData( i, 0, None )
            elif data_t1_c6[i] > 99999999 or data_t1_c6[i] < -999999:
                continue
            else:
                tdSql.checkData( i, 0, int(data_t1_c6[i]) )

        tdLog.printNoPrefix("==========step25: cast double to binary, expect changes to str(int) ")
        tdSql.query(f"select cast(c6 as binary(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c6)):
            tdSql.checkData( i, 0, None )  if data_ct4_c6[i] is None else  tdSql.checkData( i, 0, f'{data_ct4_c6[i]:.6f}' )
        tdSql.query(f"select cast(c6 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c6)):
            tdSql.checkData( i, 0, None )  if data_t1_c6[i] is None else  tdSql.checkData( i, 0, f'{data_t1_c6[i]:.6f}' )

        tdLog.printNoPrefix("==========step26: cast double to nchar, expect changes to str(int) ")
        tdSql.query(f"select cast(c6 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c6)):
            tdSql.checkData( i, 0, None )  if data_ct4_c6[i] is None else  tdSql.checkData( i, 0, f'{data_ct4_c6[i]:.6f}' )
        tdSql.query(f"select cast(c6 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c6)):
            tdSql.checkData( i, 0, None )  if data_t1_c6[i] is None else  tdSql.checkData( i, 0, f'{data_t1_c6[i]:.6f}' )

        tdLog.printNoPrefix("==========step27: cast double to timestamp, expect changes to timestamp ")
        tdSql.query(f"select cast(c6 as timestamp) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c6)):
            if data_ct4_c6[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_ct4_c6[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)

        tdSql.query(f"select cast(c6 as timestamp) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c6)):
            if data_t1_c6[i] is None:
                tdSql.checkData( i, 0 , None )
            elif i == 10:
                continue
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c6[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)

        tdLog.printNoPrefix("==========step28: cast bool to bigint, expect no changes")
        tdSql.query(f"select c7  from {self.dbname}.ct4")
        data_ct4_c7 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c7  from {self.dbname}.t1")
        data_t1_c7 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        tdSql.query(f"select cast(c7 as bigint) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c7)):
            tdSql.checkData( i, 0, data_ct4_c7[i])
        tdSql.query(f"select cast(c7 as bigint) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c7)):
            tdSql.checkData( i, 0, data_t1_c7[i])

        tdLog.printNoPrefix("==========step29: cast bool to binary, expect changes to str(int) ")
        tdSql.query(f"select cast(c7 as binary(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c7)):
            tdSql.checkData( i, 0, None ) if data_ct4_c7[i] is None else tdSql.checkData( i, 0, str(data_ct4_c7[i]).lower() )
        tdSql.query(f"select cast(c7 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c7)):
            tdSql.checkData( i, 0, None ) if data_t1_c7[i] is None else tdSql.checkData( i, 0, str(data_t1_c7[i]).lower() )

        tdLog.printNoPrefix("==========step30: cast bool to nchar, expect changes to str(int) ")
        tdSql.query(f"select cast(c7 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c7)):
            tdSql.checkData( i, 0, None ) if data_ct4_c7[i] is None else tdSql.checkData( i, 0, str(data_ct4_c7[i]).lower() )
        tdSql.query(f"select cast(c7 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c7)):
            tdSql.checkData( i, 0, None ) if data_t1_c7[i] is None else tdSql.checkData( i, 0, str(data_t1_c7[i]).lower() )

        tdLog.printNoPrefix("==========step31: cast bool to timestamp, expect changes to timestamp ")
        tdSql.query(f"select cast(c7 as timestamp) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c7)):
            if data_ct4_c7[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_ct4_c7[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)
        tdSql.query(f"select cast(c7 as timestamp) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c7)):
            if data_t1_c7[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c7[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)


        tdSql.query(f"select c8  from {self.dbname}.ct4")
        data_ct4_c8 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c8  from {self.dbname}.t1")
        data_t1_c8 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        tdLog.printNoPrefix("==========step32: cast binary to binary, expect no changes ")
        tdSql.query(f"select cast(c8 as binary(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c8)):
            tdSql.checkData( i, 0, None ) if data_ct4_c8[i] is None else  tdSql.checkData(i,0,data_ct4_c8[i])

        tdSql.query(f"select cast(c8 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c8)):
            tdSql.checkData( i, 0, None ) if data_t1_c8[i] is None else  tdSql.checkData(i,0,data_t1_c8[i])

        tdLog.printNoPrefix("==========step33: cast binary to binary, expect truncate ")
        tdSql.query(f"select cast(c8 as binary(2)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c8)):
            if data_ct4_c8[i] is None:
                tdSql.checkData( i, 0, None)
            elif tdSql.getData(i,0) == data_ct4_c8[i][:2]:
                tdLog.info( f"sql:{tdSql.sql}, row:{i} col:0 data:{tdSql.queryResult[i][0]} == expect:{data_ct4_c8[i][:2]}" )
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{tdSql.sql} row:{i} col:0 data:{tdSql.queryResult[i][0]} != expect:{data_ct4_c8[i][:2]}")
        tdSql.query(f"select cast(c8 as binary(2)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c8)):
            if data_t1_c8[i] is None:
                tdSql.checkData( i, 0, None)
            elif tdSql.getData(i,0) == data_t1_c8[i][:2]:
                tdLog.info( f"sql:{tdSql.sql}, row:{i} col:0 data:{tdSql.queryResult[i][0]} == expect:{data_t1_c8[i][:2]}" )
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{tdSql.sql} row:{i} col:0 data:{tdSql.queryResult[i][0]} != expect:{data_t1_c8[i][:2]}")

        tdLog.printNoPrefix("==========step34: cast binary to nchar, expect changes to str(int) ")
        tdSql.query(f"select cast(c8 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c8)):
            if data_ct4_c8[i] is None:
                tdSql.checkData( i, 0, None)
            elif tdSql.getData(i,0) == data_ct4_c8[i]:
                tdLog.info( f"sql:{tdSql.sql}, row:{i} col:0 data:{tdSql.queryResult[i][0]} == expect:{data_ct4_c8[i]}" )
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{tdSql.sql} row:{i} col:0 data:{tdSql.queryResult[i][0]} != expect:{data_ct4_c8[i]}")
        tdSql.query(f"select cast(c8 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c8)):
            if data_t1_c8[i] is None:
                tdSql.checkData( i, 0, None)
            elif tdSql.getData(i,0) == data_t1_c8[i]:
                tdLog.info( f"sql:{tdSql.sql}, row:{i} col:0 data:{tdSql.queryResult[i][0]} == expect:{data_t1_c8[i]}" )
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{tdSql.sql} row:{i} col:0 data:{tdSql.queryResult[i][0]} != expect:{data_t1_c8[i]}")


        tdSql.query(f"select c9  from {self.dbname}.ct4")
        data_ct4_c9 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c9  from {self.dbname}.t1")
        data_t1_c9 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        "c10 timestamp"

        tdLog.printNoPrefix("==========step35: cast nchar to nchar, expect no changes ")
        tdSql.query(f"select cast(c9 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c9)):
            if data_ct4_c9[i] is None:
                tdSql.checkData( i, 0, None)
            elif tdSql.getData(i,0) == data_ct4_c9[i]:
                tdLog.info( f"sql:{tdSql.sql}, row:{i} col:0 data:{tdSql.queryResult[i][0]} == expect:{data_ct4_c9[i]}" )
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{tdSql.sql} row:{i} col:0 data:{tdSql.queryResult[i][0]} != expect:{data_ct4_c9[i]}")
        tdSql.query(f"select cast(c9 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c9)):
            tdSql.checkData( i, 0, data_t1_c9[i] )
            if data_t1_c9[i] is None:
                tdSql.checkData( i, 0, None)
            elif tdSql.getData(i,0) == data_t1_c9[i]:
                tdLog.info( f"sql:{tdSql.sql}, row:{i} col:0 data:{tdSql.queryResult[i][0]} == expect:{data_t1_c9[i]}" )
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{tdSql.sql} row:{i} col:0 data:{tdSql.queryResult[i][0]} != expect:{data_t1_c9[i]}")

        tdLog.printNoPrefix("==========step36: cast nchar to nchar, expect truncate ")
        tdSql.query(f"select cast(c9 as nchar(2)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c9)):
            if data_ct4_c9[i] is None:
                tdSql.checkData( i, 0, None)
            elif tdSql.getData(i,0) == data_ct4_c9[i][:2]:
                tdLog.info( f"sql:{tdSql.sql}, row:{i} col:0 data:{tdSql.queryResult[i][0]} == expect:{data_ct4_c9[i][:2]}" )
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{tdSql.sql} row:{i} col:0 data:{tdSql.queryResult[i][0]} != expect:{data_ct4_c9[i][:2]}")
        tdSql.query(f"select cast(c9 as nchar(2)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c9)):
            if data_t1_c9[i] is None:
                tdSql.checkData( i, 0, None)
            elif tdSql.getData(i,0) == data_t1_c9[i][:2]:
                tdLog.info( f"sql:{tdSql.sql}, row:{i} col:0 data:{tdSql.queryResult[i][0]} == expect:{data_t1_c9[i][:2]}" )
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{tdSql.sql} row:{i} col:0 data:{tdSql.queryResult[i][0]} != expect:{data_t1_c9[i][:2]}")

        tdSql.query(f"select c10  from {self.dbname}.ct4")
        data_ct4_c10 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
        tdSql.query(f"select c10  from {self.dbname}.t1")
        data_t1_c10 = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]

        tdLog.printNoPrefix("==========step37: cast timestamp to nchar, expect no changes ")
        tdSql.query(f"select cast(c10 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c10)):
            if data_ct4_c10[i] is None:
                tdSql.checkData( i, 0, None )
            else:
                time2str = str(int((data_ct4_c10[i]-datetime.datetime.fromtimestamp(0,data_ct4_c10[i].tzinfo)).total_seconds())*1000+int(data_ct4_c10[i].microsecond / 1000))
                tdSql.checkData( i, 0, time2str )
        tdSql.query(f"select cast(c10 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c10)):
            if data_t1_c10[i] is None:
                tdSql.checkData( i, 0, None )
            elif i == 10:
                continue
            else:
                time2str = str(int((data_t1_c10[i]-datetime.datetime.fromtimestamp(0,data_t1_c10[i].tzinfo)).total_seconds())*1000+int(data_t1_c10[i].microsecond / 1000))
                tdSql.checkData( i, 0, time2str )

        tdLog.printNoPrefix("==========step38: cast timestamp to binary, expect no changes ")
        tdSql.query(f"select cast(c10 as binary(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c10)):
            if data_ct4_c10[i] is None:
                tdSql.checkData( i, 0, None )
            else:
                time2str = str(int((data_ct4_c10[i]-datetime.datetime.fromtimestamp(0,data_ct4_c10[i].tzinfo)).total_seconds())*1000+int(data_ct4_c10[i].microsecond / 1000))
                tdSql.checkData( i, 0, time2str )
        tdSql.query(f"select cast(c10 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c10)):
            if data_t1_c10[i] is None:
                tdSql.checkData( i, 0, None )
            elif i == 10:
                continue
            else:
                time2str = str(int((data_t1_c10[i]-datetime.datetime.fromtimestamp(0,data_t1_c10[i].tzinfo)).total_seconds())*1000+int(data_t1_c10[i].microsecond / 1000))
                tdSql.checkData( i, 0, time2str )

        tdLog.printNoPrefix("==========step39: cast constant operation to bigint, expect change to int ")
        tdSql.query(f"select cast(12121.23323131  as bigint) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, 12121) for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131  as binary(16)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12121.233231') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131  as binary(2)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131  as nchar(16)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12121.233231') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131  as nchar(2)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12') for i in range(tdSql.queryRows) )

        tdSql.query(f"select cast(12121.23323131 + 321.876897998  as bigint) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, 12443) for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 321.876897998  as binary(16)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12443.110129') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 321.876897998  as binary(3)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '124') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 321.876897998  as nchar(16)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12443.110129') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 321.876897998  as nchar(3)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '124') for i in range(tdSql.queryRows) )

        tdSql.query(f"select cast(12121.23323131 + 'test~!@`#$%^&*(){'}'}{'{'}][;><.,' as bigint) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, 12121) for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 'test~!@`#$%^&*(){'}'}{'{'}][;><.,' as binary(16)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12121.233231') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 'test~!@`#$%^&*(){'}'}{'{'}][;><.,' as binary(2)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 'test~!@`#$%^&*(){'}'}{'{'}][;><.,' as nchar(16)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12121.233231') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 'test~!@`#$%^&*(){'}'}{'{'}][;><.,' as nchar(2)) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12') for i in range(tdSql.queryRows) )

        tdLog.printNoPrefix("==========step40: current cast condition, should return ok ")
        tdSql.query(f"select cast(c1 as int) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c1 as bool) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c1 as tinyint) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c1 as smallint) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c1 as float) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c1 as double) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c1 as tinyint unsigned) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c1 as smallint unsigned) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c1 as int unsigned) as b from {self.dbname}.ct4")

        tdSql.query(f"select cast(c2 as int) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c3 as bool) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c4 as tinyint) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c5 as smallint) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c6 as float) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c7 as double) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c8 as tinyint unsigned) as b from {self.dbname}.ct4")

        tdSql.query(f"select cast(c8 as timestamp ) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c9 as timestamp ) as b from {self.dbname}.ct4")
        tdSql.query(f"select cast(c9 as binary(64) ) as b from {self.dbname}.ct4")

        # enh of cast function about coverage

        tdSql.query(f"select cast(c1 as int) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c1 as bool) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c1 as tinyint) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c1 as smallint) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c1 as float) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c1 as double) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c1 as tinyint unsigned) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c1 as smallint unsigned) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c1 as int unsigned) as b from {self.dbname}.stb1")

        tdSql.query(f"select cast(c2 as int) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c3 as bool) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c4 as tinyint) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c5 as smallint) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c6 as float) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c7 as double) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c8 as tinyint unsigned) as b from {self.dbname}.stb1")

        tdSql.query(f"select cast(c8 as timestamp ) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c9 as timestamp ) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c9 as binary(64) ) as b from {self.dbname}.stb1")

        tdSql.query(f"select cast(abs(c2) as int) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c3 as bool) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(floor(c4) as tinyint) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c5+2 as smallint) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(2 as float) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c7 as double) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast('123' as tinyint unsigned) as b from {self.dbname}.stb1")

        tdSql.query(f"select max(cast(abs(c2) as int)) as b from {self.dbname}.stb1")
        tdSql.query(f"select log(cast(c3 as int),2) as b from {self.dbname}.stb1")
        tdSql.query(f"select abs(cast(floor(c4) as tinyint)) as b from {self.dbname}.stb1")
        tdSql.query(f"select last(cast(c5+2 as smallint)) as b from {self.dbname}.stb1")
        tdSql.query(f"select mavg(cast(2 as float),3) as b from {self.dbname}.stb1 partition by tbname")
        tdSql.query(f"select cast(c7 as double) as b from {self.dbname}.stb1 partition by tbname order by tbname")
        tdSql.query(f"select cast('123' as tinyint unsigned) as b from {self.dbname}.stb1 partition by tbname")

        # uion with cast and common cols

        tdSql.query(f"select cast(c2 as int) as b from {self.dbname}.stb1 union all select c1 from {self.dbname}.stb1 ")
        tdSql.query(f"select cast(c3 as bool) as b from {self.dbname}.stb1 union all select c7 from {self.dbname}.ct1 ")
        tdSql.query(f"select cast(c4 as tinyint) as b from {self.dbname}.stb1 union all select c4 from {self.dbname}.stb1")
        tdSql.query(f"select cast(c5 as smallint) as b from {self.dbname}.stb1 union all select cast(c5 as smallint) as b from {self.dbname}.stb1")
        tdSql.query(f"select cast(c6 as float) as b from {self.dbname}.stb1 union all select c5 from {self.dbname}.stb1")
        tdSql.query(f"select cast(c7 as double) as b from {self.dbname}.stb1 union all select 123 from {self.dbname}.stb1 ")
        tdSql.query(f"select cast(c8 as tinyint unsigned) as b from {self.dbname}.stb1 union all select last(cast(c8 as tinyint unsigned)) from {self.dbname}.stb1")



    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        tdSql.execute(
            f'''create table {self.dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )
        tdSql.execute(
            f'''
            create table {self.dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(f'create table {self.dbname}.ct{i+1} using {self.dbname}.stb1 tags ( {i+1} )')

        tdLog.printNoPrefix("==========step2:insert data")
        for i in range(9):
            tdSql.execute(
                f"insert into {self.dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {self.dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(f"insert into {self.dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {self.dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {self.dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {self.dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {self.dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into {self.dbname}.t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

        self.all_test()

        # tdDnodes.stop(1)
        # tdDnodes.start(1)

        tdSql.execute(f"flush database {self.dbname}")

        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
