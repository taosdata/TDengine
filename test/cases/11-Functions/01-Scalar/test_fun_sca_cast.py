from new_test_framework.utils import tdLog, tdSql
import datetime
import inspect
import sys


class TestFunCast:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.dbname = "db"
        cls._datetime_epoch = datetime.datetime.fromtimestamp(0)

    #
    # ----------------------- sim -------------------
    #
    def do_cast_const(self):
        tdLog.info(f"======== step1")
        tdSql.prepare("db1", drop=True, vgroups=3)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable st1 (fts timestamp, fbool bool, ftiny tinyint, fsmall smallint, fint int, fbig bigint, futiny tinyint unsigned, fusmall smallint unsigned, fuint int unsigned, fubig bigint unsigned, ffloat float, fdouble double, fbin binary(10), fnchar nchar(10)) tags(tts timestamp, tbool bool, ttiny tinyint, tsmall smallint, tint int, tbig bigint, tutiny tinyint unsigned, tusmall smallint unsigned, tuint int unsigned, tubig bigint unsigned, tfloat float, tdouble double, tbin binary(10), tnchar nchar(10));"
        )
        tdSql.execute(
            f"create table tb1 using st1 tags('2022-07-10 16:31:00', true, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a');"
        )
        tdSql.execute(
            f"create table tb2 using st1 tags('2022-07-10 16:32:00', false, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b');"
        )
        tdSql.execute(
            f"create table tb3 using st1 tags('2022-07-10 16:33:00', true, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c');"
        )

        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a');"
        )
        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b');"
        )
        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c');"
        )
        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd');"
        )
        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e');"
        )

        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a');"
        )
        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b');"
        )
        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c');"
        )
        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd');"
        )
        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e');"
        )

        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a');"
        )
        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b');"
        )
        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c');"
        )
        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd');"
        )
        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e');"
        )

        tdSql.query(f"select 1+1n;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.000000000)

        tdSql.query(f"select cast(1 as timestamp)+1n;")
        tdSql.checkRows(1)
        tdLog.info(f"==== {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "1970-02-01 08:00:00.001000")

        tdSql.query(
            f"select cast('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as double);"
        )
        tdSql.query(f"select 1-1n;")
        tdSql.checkRows(1)

        # there is an *bug* in print timestamp that smaller than 0, so let's try value that is greater than 0.
        tdSql.query(f"select cast(1 as timestamp)+1y;")
        tdSql.checkRows(1)
        tdLog.info(f"==== {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, "1971-01-01 08:00:00.001000")

        tdSql.query(f"select 1n-now();")
        tdSql.query(f"select 1n+now();")
    
    #
    # ----------------------- system-test -------------------
    #
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
            
        tdSql.query(f"select cast(c1 as binary) as b from {self.dbname}.t1")
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
            
        tdSql.query(f"select cast(c2 as binary) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c2)):
            tdSql.checkData( i, 0, str(data_ct4_c2[i]) )
        tdSql.query(f"select cast(c2 as binary) as b from {self.dbname}.t1")
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


        # tdSql.query(f"select cast(c2 as timestamp) as b from {self.dbname}.t1")
        # for i in range(len(data_t1_c2)):
        #     if data_t1_c2[i] is None:
        #         tdSql.checkData( i, 0 , None )
        #     elif i == 10:
        #         continue
        #     else:
        #         date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c2[i]) / 1000.0)
        #         tdSql.checkData( i, 0, date_init_stamp)


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
            
        tdSql.query(f"select cast(c3 as binary) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c3)):
            tdSql.checkData( i, 0, str(data_ct4_c3[i]) )
        tdSql.query(f"select cast(c3 as binary) as b from {self.dbname}.t1")
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
            
        tdSql.query(f"select cast(c4 as binary) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c4)):
            tdSql.checkData( i, 0, str(data_ct4_c4[i]) )
        tdSql.query(f"select cast(c4 as binary) as b from {self.dbname}.t1")
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
            tdSql.checkData( i, 0, str(data_ct4_c5[i]) )  if data_ct4_c5[i] is None else  tdSql.checkFloatString( i, 0, data_ct4_c5[i])
        tdSql.query(f"select cast(c5 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            tdSql.checkData( i, 0, str(data_t1_c5[i]) )  if data_t1_c5[i] is None else tdSql.checkFloatString( i, 0, data_t1_c5[i])
        tdSql.query(f"select cast(c5 as binary) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c5)):
            tdSql.checkData( i, 0, str(data_ct4_c5[i]) )  if data_ct4_c5[i] is None else  tdSql.checkFloatString( i, 0, data_ct4_c5[i])
        tdSql.query(f"select cast(c5 as binary) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            tdSql.checkData( i, 0, str(data_t1_c5[i]) )  if data_t1_c5[i] is None else tdSql.checkFloatString( i, 0, data_t1_c5[i])

        tdLog.printNoPrefix("==========step22: cast float to nchar, expect changes to str(int) ")
        tdSql.query(f"select cast(c5 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c5)):
            tdSql.checkData( i, 0, None )  if data_ct4_c5[i] is None else  tdSql.checkFloatString( i, 0, data_ct4_c5[i])
        tdSql.query(f"select cast(c5 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            tdSql.checkData( i, 0, None )  if data_t1_c5[i] is None else tdSql.checkFloatString( i, 0, data_t1_c5[i])
        tdSql.query(f"select cast(c5 as nchar) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            tdSql.checkData( i, 0, None )  if data_t1_c5[i] is None else tdSql.checkFloatString( i, 0, data_t1_c5[i])
        tdSql.query(f"select cast(c5 as varchar) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c5)):
            tdSql.checkData( i, 0, None )  if data_t1_c5[i] is None else tdSql.checkFloatString( i, 0, data_t1_c5[i])

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
            tdSql.checkData( i, 0, None )  if data_ct4_c6[i] is None else  tdSql.checkFloatString( i, 0, data_ct4_c6[i])
        tdSql.query(f"select cast(c6 as binary(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c6)):
            tdSql.checkData( i, 0, None )  if data_t1_c6[i] is None else  tdSql.checkFloatString( i, 0, data_t1_c6[i])

        tdLog.printNoPrefix("==========step26: cast double to nchar, expect changes to str(int) ")
        tdSql.query(f"select cast(c6 as nchar(32)) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c6)):
            tdSql.checkData( i, 0, None )  if data_ct4_c6[i] is None else  tdSql.checkFloatString( i, 0, data_ct4_c6[i])
        tdSql.query(f"select cast(c6 as nchar(32)) as b from {self.dbname}.t1")
        for i in range(len(data_t1_c6)):
            tdSql.checkData( i, 0, None )  if data_t1_c6[i] is None else  tdSql.checkFloatString( i, 0, data_t1_c6[i])

        tdLog.printNoPrefix("==========step27: cast double to timestamp, expect changes to timestamp ")
        tdSql.query(f"select cast(c6 as timestamp) as b from {self.dbname}.ct4")
        for i in range(len(data_ct4_c6)):
            if data_ct4_c6[i] is None:
                tdSql.checkData( i, 0 , None )
            else:
                date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_ct4_c6[i]) / 1000.0)
                tdSql.checkData( i, 0, date_init_stamp)

        # tdSql.query(f"select cast(c6 as timestamp) as b from {self.dbname}.t1")
        # for i in range(len(data_t1_c6)):
        #     if data_t1_c6[i] is None:
        #         tdSql.checkData( i, 0 , None )
        #     elif i == 10:
        #         continue
        #     else:
        #         date_init_stamp = _datetime_epoch+datetime.timedelta(seconds=int(data_t1_c6[i]) / 1000.0)
        #         tdSql.checkData( i, 0, date_init_stamp)

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
        tdSql.query(f"select cast(12121.23323131 + 'test~!@`#$%^&*(){'}'}{'{'}][;><.,' as binary) as b from {self.dbname}.ct4")
        ( tdSql.checkData(i, 0, '12121.233231') for i in range(tdSql.queryRows) )
        tdSql.query(f"select cast(12121.23323131 + 'test~!@`#$%^&*(){'}'}{'{'}][;><.,' as binary) as b from {self.dbname}.ct4")
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

    def do_system_test_cast(self):
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
        
        print("do system-test cast ................... [passed]")

    #
    # ----------------------- army -------------------
    #
    def cast_from_int_to_other(self):
        # int
        int_num1 = 2147483647
        int_num2 = 2147483648
        tdSql.query(f"select cast({int_num1} as int) re;")
        tdSql.checkData(0, 0, int_num1)

        tdSql.query(f"select cast({int_num2} as int) re;")
        tdSql.checkData(0, 0, -int_num2)

        tdSql.query(f"select cast({int_num2} as int unsigned) re;")
        tdSql.checkData(0, 0, int_num2)

        tdSql.query(f"select cast({int_num1} as bigint) re;")
        tdSql.checkData(0, 0, int_num1)

        tdSql.query(f"select cast({int_num1} as bigint unsigned) re;")
        tdSql.checkData(0, 0, int_num1)

        tdSql.query(f"select cast({int_num1} as smallint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({int_num1} as smallint unsigned) re;")
        tdSql.checkData(0, 0, 65535)

        tdSql.query(f"select cast({int_num1} as tinyint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({int_num1} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast({int_num2} as float) re;")
        tdSql.checkData(0, 0, "2147483648.0")

        tdSql.query(f"select cast({int_num2} as double) re;")
        tdSql.checkData(0, 0, "2147483648.0")

        tdSql.query(f"select cast({int_num2} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({int_num2} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(int_num2) / 1000))

        tdSql.query(f"select cast({int_num1} as varchar(5)) as re;")
        tdSql.checkData(0, 0, "21474")

        tdSql.query(f"select cast({int_num1} as binary(5)) as re;")
        tdSql.checkData(0, 0, "21474")

        tdSql.query(f"select cast({int_num1} as nchar(5));")
        tdSql.checkData(0, 0, "21474")

    def cast_from_bigint_to_other(self):
        # bigint
        bigint_num = 9223372036854775807
        bigint_num2 = 9223372036854775808
        tdSql.query(f"select cast({bigint_num} as int) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({bigint_num} as int unsigned) re;")
        tdSql.checkData(0, 0, 4294967295)

        tdSql.query(f"select cast({bigint_num} as bigint) re;")
        tdSql.checkData(0, 0, bigint_num)

        tdSql.query(f"select cast({bigint_num2} as bigint) re;")
        tdSql.checkData(0, 0, -bigint_num2)

        tdSql.query(f"select cast({bigint_num2} as bigint unsigned) re;")
        tdSql.checkData(0, 0, bigint_num2)

        tdSql.query(f"select cast({bigint_num} as smallint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({bigint_num} as smallint unsigned) re;")
        tdSql.checkData(0, 0, 65535)

        tdSql.query(f"select cast({bigint_num} as tinyint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({bigint_num} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast({bigint_num} as float) re;")
        tdSql.checkData(0, 0, 9.2233720e18)

        tdSql.query(f"select cast({bigint_num} as double) re;")
        tdSql.checkData(0, 0, 9.2233720e18)

        tdSql.query(f"select cast({bigint_num} as bool) as re;")
        tdSql.checkData(0, 0, True)

        # WARN: datetime overflow dont worry
        tdSql.query(f"select cast({bigint_num} as timestamp) as re;")
        # tdSql.checkData(0, 0, "292278994-08-17 15:12:55.807")

        tdSql.query(f"select cast({bigint_num} as varchar(5)) as re;")
        tdSql.checkData(0, 0, "92233")

        tdSql.query(f"select cast({bigint_num} as binary(5)) as re;")
        tdSql.checkData(0, 0, "92233")

        tdSql.query(f"select cast({bigint_num} as nchar(5));")
        tdSql.checkData(0, 0, "92233")

    def cast_from_smallint_to_other(self):
        smallint_num = 32767
        smallint_num2 = 32768
        tdSql.query(f"select cast({smallint_num} as int) re;")
        tdSql.checkData(0, 0, smallint_num)

        tdSql.query(f"select cast({smallint_num} as int unsigned) re;")
        tdSql.checkData(0, 0, smallint_num)

        tdSql.query(f"select cast({smallint_num} as bigint) re;")
        tdSql.checkData(0, 0, smallint_num)

        tdSql.query(f"select cast({smallint_num2} as bigint unsigned) re;")
        tdSql.checkData(0, 0, smallint_num2)

        tdSql.query(f"select cast({smallint_num} as smallint) re;")
        tdSql.checkData(0, 0, smallint_num)

        tdSql.query(f"select cast({smallint_num2} as smallint) re;")
        tdSql.checkData(0, 0, -smallint_num2)

        tdSql.query(f"select cast({smallint_num2} as smallint unsigned) re;")
        tdSql.checkData(0, 0, smallint_num2)

        tdSql.query(f"select cast({smallint_num} as tinyint) re;")
        tdSql.checkData(0, 0, -1)

        tdSql.query(f"select cast({smallint_num} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast({smallint_num} as float) re;")
        tdSql.checkData(0, 0, "32767.0")

        tdSql.query(f"select cast({smallint_num} as double) re;")
        tdSql.checkData(0, 0, "32767.0")

        tdSql.query(f"select cast({smallint_num} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({smallint_num} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(smallint_num) / 1000))

        tdSql.query(f"select cast({smallint_num} as varchar(3)) as re;")
        tdSql.checkData(0, 0, "327")

        tdSql.query(f"select cast({smallint_num} as binary(3)) as re;")
        tdSql.checkData(0, 0, "327")

        tdSql.query(f"select cast({smallint_num} as nchar(3));")
        tdSql.checkData(0, 0, "327")

    def cast_from_tinyint_to_other(self):
        tinyint_num = 127
        tinyint_num2 = 128
        tdSql.query(f"select cast({tinyint_num} as int) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num} as int unsigned) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num} as bigint) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num2} as bigint unsigned) re;")
        tdSql.checkData(0, 0, tinyint_num2)

        tdSql.query(f"select cast({tinyint_num} as smallint) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num} as smallint unsigned) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num} as tinyint) re;")
        tdSql.checkData(0, 0, tinyint_num)

        tdSql.query(f"select cast({tinyint_num2} as tinyint) re;")
        tdSql.checkData(0, 0, -tinyint_num2)

        tdSql.query(f"select cast({tinyint_num2} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, tinyint_num2)

        tdSql.query(f"select cast({tinyint_num} as float) re;")
        tdSql.checkData(0, 0, "127.0")

        tdSql.query(f"select cast({tinyint_num} as double) re;")
        tdSql.checkData(0, 0, "127.0")

        tdSql.query(f"select cast({tinyint_num} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({tinyint_num} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(tinyint_num) / 1000))

        tdSql.query(f"select cast({tinyint_num} as varchar(2)) as re;")
        tdSql.checkData(0, 0, "12")

        tdSql.query(f"select cast({tinyint_num} as binary(2)) as re;")
        tdSql.checkData(0, 0, "12")

        tdSql.query(f"select cast({tinyint_num} as nchar(2));")
        tdSql.checkData(0, 0, "12")

    def cast_from_float_to_other(self):
        # float
        float_1001 = 3.14159265358979323846264338327950288419716939937510582097494459230781640628620899862803482534211706798214808651328230664709384460955058223172535940812848111745028410270193852110555964462294895493038196442881097566593344612847564823378678316527120190914564856692346034861045432664821339360726024914127372458700660631558817488152092096282925409171536436789259036001133053054882046652138414695194151160943305727036575959195309218611738193261179310511854807446237996274956735188575272489122793818301194912983367336244065664308602139494639522473719070217986094370277053921717629317675238467481846766940513200056812714526356082778577134275778960917363717872146844090122495343014654958537105079227968925892354201995611212902196086403441815981362977477130996051870721134999999837297804995105973173281609631859502445945534690830264252230825334468503526193118817101000313783875288658753320838142061717766914730359825349042875546873115956286388235378759375195778185778053217122680661300192787661119590921642019

        tdSql.query(f"select cast({float_1001} as int) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as int unsigned) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as tinyint) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as tinyint unsigned) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as smallint) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as smallint unsigned) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as bigint) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as bigint unsigned) as re;")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select cast({float_1001} as double) as re;")
        tdSql.checkData(0, 0, 3.141592653589793)

        tdSql.query(f"select cast({float_1001} as float) as re;")
        tdSql.checkData(0, 0, 3.1415927)

        tdSql.query(f"select cast({float_1001} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({float_1001} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(float_1001) / 1000))

        sql = f"select cast({float_1001} as varchar(5)) as re;"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 3.141)

        sql = f"select cast({float_1001} as binary(10)) as re;"
        tdLog.debug(sql)
        tdSql.query(sql)
        tdSql.checkData(0, 0, 3.14159265)

        tdSql.query(f"select cast({float_1001} as nchar(5));")
        tdSql.checkData(0, 0, 3.141)

    def cast_from_str_to_other(self):
        # str
        _str = "bcdefghigk"
        str_410 = _str * 41
        str_401 = _str * 40 + "b"
        big_str = _str * 6552

        tdSql.query(f"select cast('{str_410}' as binary(401)) as re;")
        tdSql.checkData(0, 0, str_401)

        tdSql.query(f"select cast('{str_410}' as varchar(401)) as re;")
        tdSql.checkData(0, 0, str_401)

        tdSql.query(f"select cast('{big_str}' as varchar(420)) as re;")
        tdSql.checkData(0, 0, _str * 42)

        tdSql.query(f"select cast('{str_410}' as nchar(401));")
        tdSql.checkData(0, 0, str_401)

        tdSql.query(f"select cast('北京' as nchar(10));")
        tdSql.checkData(0, 0, "北京")

        # tdSql.query(f"select cast('北京涛思数据有限公司' as nchar(6));")
        # tdSql.checkData(0, 0, "北京涛思数据")

        tdSql.query(f"select cast('{str_410}' as int) as re;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as int unsigned) as re;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as tinyint) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as tinyint unsigned) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as smallint) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as smallint unsigned) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as bigint) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as bigint unsigned) as re")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select cast('{str_410}' as float) as re;")
        tdSql.checkData(0, 0, 0.0000000)

        tdSql.query(f"select cast('{str_410}' as double) as re;")
        tdSql.checkData(0, 0, 0.000000000000000)

        tdSql.query(f"select cast('{str_410}' as bool) as re")
        tdSql.checkData(0, 0, False)

        tdSql.query(f"select cast('{str_410}' as timestamp) as re")
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.000")

    def cast_from_bool_to_other(self):
        true_val = True
        false_val = False
        tdSql.query(f"select cast({false_val} as int) re, cast({true_val} as int) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as int unsigned) re, cast({true_val} as int unsigned) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as bigint) re, cast({true_val} as bigint) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select cast({false_val} as bigint unsigned) re, cast({true_val} as bigint unsigned) re;"
        )
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as smallint) re, cast({true_val} as smallint) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select cast({false_val} as smallint unsigned) re, cast({true_val} as smallint unsigned) re;"
        )
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as tinyint) re, cast({true_val} as tinyint) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(
            f"select cast({false_val} as tinyint unsigned) re, cast({true_val} as tinyint unsigned) re;"
        )
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as smallint) re, cast({true_val} as smallint) re;")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select cast({false_val} as float) re, cast({true_val} as float) re;")
        tdSql.checkData(0, 0, 0.0000000)
        tdSql.checkData(0, 1, 1.0000000)

        tdSql.query(f"select cast({false_val} as double) re, cast({true_val} as double) re;")
        tdSql.checkData(0, 0, 0.000000000000000)
        tdSql.checkData(0, 1, 1.000000000000000)

        tdSql.query(f"select cast({false_val} as bool) re, cast({true_val} as bool) re;")
        tdSql.checkData(0, 0, false_val)
        tdSql.checkData(0, 1, true_val)

        tdSql.query(f"select cast({false_val} as timestamp) re, cast({true_val} as timestamp) re;")
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.000")
        tdSql.checkData(0, 1, "1970-01-01 08:00:00.001")

        tdSql.query(f"select cast({false_val} as varchar(3)) re, cast({true_val} as varchar(3)) re;")
        tdSql.checkData(0, 0, "fal")
        tdSql.checkData(0, 1, "tru")

        tdSql.query(f"select cast({false_val} as binary(3)) re, cast({true_val} as binary(3)) re;")
        tdSql.checkData(0, 0, "fal")
        tdSql.checkData(0, 1, "tru")

        tdSql.query(f"select cast({false_val} as nchar(3)) re, cast({true_val} as nchar(3)) re;")
        tdSql.checkData(0, 0, "fal")
        tdSql.checkData(0, 1, "tru")

    def cast_from_timestamp_to_other(self):
        # ts = self._datetime_epoch
        # tdSql.query(f"select cast({ts} as int) re;")
        # tdSql.checkData(0, 0, None)
        # todo
        pass

    def cast_from_null_to_other(self):
        tdSql.query(f"select cast(null as int) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as int unsigned) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as bigint) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as bigint unsigned) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as smallint) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as smallint unsigned) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as tinyint) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as tinyint unsigned) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as float) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as double) re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as bool) as re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as timestamp) as re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as varchar(55)) as re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as binary(5)) as re;")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(null as nchar(5));")
        tdSql.checkData(0, 0, None)

    def cast_from_compute_to_other(self):
        add1 = 123
        add2 = 456
        re = 579
        tdSql.query(f"select cast({add1}+{add2} as int) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as int unsigned) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as bigint) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as bigint unsigned) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as smallint) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as smallint unsigned) re;")
        tdSql.checkData(0, 0, re)

        tdSql.query(f"select cast({add1}+{add2} as tinyint) re;")
        tdSql.checkData(0, 0, 67)

        tdSql.query(f"select cast({add1}+{add2} as tinyint unsigned) re;")
        tdSql.checkData(0, 0, 67)

        tdSql.query(f"select cast({add1}+{add2} as float) re;")
        tdSql.checkData(0, 0, "579.0")

        tdSql.query(f"select cast({add1}+{add2} as double) re;")
        tdSql.checkData(0, 0, "579.0")

        tdSql.query(f"select cast({add1}+{add2} as bool) as re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({add1}+{add2} as timestamp) as re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(re) / 1000))

        tdSql.query(f"select cast({add1}+{add2} as varchar(2)) as re;")
        tdSql.checkData(0, 0, "57")

        tdSql.query(f"select cast({add1}+{add2} as binary(2)) as re;")
        tdSql.checkData(0, 0, "57")

        tdSql.query(f"select cast({add1}+{add2} as nchar(2));")
        tdSql.checkData(0, 0, "57")

        test_str = "'!@#'"
        tdSql.query(f"select cast({add1}+{test_str} as int) re;")
        tdSql.checkData(0, 0, add1)

        tdSql.query(f"select cast({add1}+{test_str} as bigint) re;")
        tdSql.checkData(0, 0, add1)

        tdSql.query(f"select cast({add1}+{test_str} as smallint) re;")
        tdSql.checkData(0, 0, add1)

        tdSql.query(f"select cast({add1}+{test_str} as tinyint) re;")
        tdSql.checkData(0, 0, add1)

        tdSql.query(f"select cast({add1}+{test_str} as float) re;")
        tdSql.checkData(0, 0, "123.0")

        tdSql.query(f"select cast({add1}+{test_str} as double) re;")
        tdSql.checkData(0, 0, "123.0")

        tdSql.query(f"select cast({add1}+{test_str} as bool) re;")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast({add1}+{test_str} as timestamp) re;")
        tdSql.checkData(0, 0, self._datetime_epoch + datetime.timedelta(seconds=int(add1) / 1000))

        tdSql.query(f"select cast({add1}+{test_str} as varchar(2)) re;")
        tdSql.checkData(0, 0, "12")

        tdSql.query(f"select cast({add1}+{test_str} as binary(2)) re;")
        tdSql.checkData(0, 0, "12")

        tdSql.query(f"select cast({add1}+{test_str} as nchar(2)) re;")
        tdSql.checkData(0, 0, "12")
        
    def ts5972(self):
        tdSql.execute("CREATE DATABASE IF NOT EXISTS ts5972;")
        tdSql.execute("DROP TABLE IF EXISTS ts5972.t1;")
        tdSql.execute("DROP TABLE IF EXISTS ts5972.t1;")
        tdSql.execute("CREATE TABLE ts5972.t1(time TIMESTAMP, c0 DOUBLE);")
        tdSql.execute("INSERT INTO ts5972.t1(time, c0) VALUES (1641024000000, 0.018518518518519), (1641024005000, 0.015151515151515), (1641024010000, 0.1234567891012345);")
        tdSql.query("SELECT c0, CAST(c0 AS BINARY(50)) FROM ts5972.t1 WHERE CAST(c0 AS BINARY(50)) != c0;")
        tdSql.checkRows(0)
        tdSql.query("SELECT c0, CAST(c0 AS BINARY(50)) FROM ts5972.t1 WHERE CAST(c0 AS BINARY(50)) == c0;")
        tdSql.checkRows(3)


    def cast_without_from(self):
        self.cast_from_int_to_other()
        self.cast_from_bigint_to_other()
        self.cast_from_smallint_to_other()
        self.cast_from_tinyint_to_other()
        self.cast_from_float_to_other()
        self.cast_from_str_to_other()
        self.cast_from_bool_to_other()
        self.cast_from_timestamp_to_other()
        self.cast_from_compute_to_other()
        # self.cast_from_null_to_other()

    def do_army_cast(self):
        # 'from table' case see system-test/2-query/cast.py
        self.cast_without_from()
        self.ts5972()

    # main
    def test_fun_sca_cast(self):
        """ Fun: cast()

        1. CAST on super table and normal table
        2. CAST between all data types
        3. CAST with null values
        4. CAST constant operation
        5. CAST with aggregation functions
        6. CAST with union all
        7. CAST with function embedded
        8. CAST without from table
        9. verify JIRA TS-5972

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-20 Alex Duan Migrated from uncatalog/army/query/function/test_cast.py
            - 2025-09-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_cast.py
            - 2025-08-23 Simon Guan Migrated function cast_const from tsim/scalar/scalar.sim
        
        """        
        self.do_cast_const()
        self.do_army_cast()
        self.do_system_test_cast()
        