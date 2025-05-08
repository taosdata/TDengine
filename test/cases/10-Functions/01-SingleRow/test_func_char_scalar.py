from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncCharScalar:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_char_scalar(self):
        """Cast 函数

        1. -

        Catalog:
            - Function:SingleRow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/charScalarFunction.sim

        """

        vgroups = 4
        dbNamme = "db"

        tdLog.info(f"=============== create database {dbNamme} vgroups {vgroups}")
        tdSql.execute(f"create database {dbNamme} vgroups {vgroups}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}"
        )
        # print $tdSql.getData(2,0) $tdSql.getData(2,1) $tdSql.getData(2,2) $tdSql.getData(2,3) $tdSql.getData(2,4) $tdSql.getData(2,5) $tdSql.getData(2,6) $tdSql.getData(2,7) $tdSql.getData(2,8) $tdSql.getData(2,9)

        tdSql.execute(f"use {dbNamme}")

        tdLog.info(f"=============== create super table")
        tdSql.execute(
            f"create table stb (ts timestamp, c1 binary(128), c2 nchar(128)) tags (t1 binary(128), t2 nchar(128))"
        )

        tdLog.info(f"=============== create child table and normal table, insert data")
        tdSql.execute(
            f'create table ctb0 using stb tags("tag-binary-0" , "tag-nchar-0" )'
        )
        tdSql.execute(
            f"create table ntb0 (ts timestamp, c1 binary(128), c2 nchar(128))"
        )
        tdSql.execute(
            f'insert into ctb0 values ("2022-01-01 00:00:00.000" , "lenByte0=11" , "lenByte0=44" )'
        )
        tdSql.execute(
            f'insert into ntb0 values ("2022-01-01 00:00:00.000" , "lenByte0=11" , "lenByte0=44" )'
        )
        tdSql.execute(
            f'insert into ctb0 values ("2022-01-01 00:00:00.001" , "lenByte01=12" , "lenByte01=48" )'
        )
        tdSql.execute(
            f'insert into ntb0 values ("2022-01-01 00:00:00.001" , "lenByte01=12" , "lenByte01=48" )'
        )
        tdSql.execute(
            f'insert into ctb0 values ("2022-01-01 00:00:00.002" , "lenChar01=12" , "lenChar01=48" )'
        )
        tdSql.execute(
            f'insert into ntb0 values ("2022-01-01 00:00:00.002" , "lenChar01=12" , "lenChar01=48" )'
        )
        tdSql.execute(
            f'insert into ctb0 values ("2022-01-01 00:00:00.003" , "lenChar0001=14" , "lenChar0001=56" )'
        )
        tdSql.execute(
            f'insert into ntb0 values ("2022-01-01 00:00:00.003" , "lenChar0001=14" , "lenChar0001=56" )'
        )

        tdSql.execute(
            f'create table ctb1 using stb tags("tag-binary-1" , "tag-nchar-1" )'
        )
        tdSql.execute(
            f"create table ntb1 (ts timestamp, c1 binary(128), c2 nchar(128))"
        )
        tdSql.execute(
            f'insert into ctb1 values ("2022-01-01 00:00:00.000" , "ABCD1234" , "ABCD1234" )'
        )
        tdSql.execute(
            f'insert into ntb1 values ("2022-01-01 00:00:00.000" , "ABCD1234" , "ABCD1234" )'
        )
        tdSql.execute(
            f'insert into ctb1 values ("2022-01-01 00:00:00.001" , "AaBbCcDd1234" , "AaBbCcDd1234" )'
        )
        tdSql.execute(
            f'insert into ntb1 values ("2022-01-01 00:00:00.001" , "AaBbCcDd1234" , "AaBbCcDd1234" )'
        )

        tdSql.execute(
            f'create table ctb2 using stb tags("tag-binary-2" , "tag-nchar-2" )'
        )
        tdSql.execute(
            f"create table ntb2 (ts timestamp, c1 binary(128), c2 nchar(128))"
        )
        tdSql.execute(
            f'insert into ctb2 values ("2022-01-01 00:00:00.000" , "abcd1234" , "abcd1234" )'
        )
        tdSql.execute(
            f'insert into ntb2 values ("2022-01-01 00:00:00.000" , "abcd1234" , "abcd1234" )'
        )
        tdSql.execute(
            f'insert into ctb2 values ("2022-01-01 00:00:00.001" , "AaBbCcDd1234" , "AaBbCcDd1234" )'
        )
        tdSql.execute(
            f'insert into ntb2 values ("2022-01-01 00:00:00.001" , "AaBbCcDd1234" , "AaBbCcDd1234" )'
        )

        tdSql.execute(
            f'create table ctb3 using stb tags("tag-binary-3" , "tag-nchar-3" )'
        )
        tdSql.execute(
            f"create table ntb3 (ts timestamp, c1 binary(128), c2 nchar(128))"
        )
        tdSql.execute(
            f'insert into ctb3 values ("2022-01-01 00:00:00.000" , "  abcd  1234  " , "  abcd  1234  " )'
        )
        tdSql.execute(
            f'insert into ntb3 values ("2022-01-01 00:00:00.000" , "  abcd  1234  " , "  abcd  1234  " )'
        )

        tdSql.execute(
            f"create table stb2 (ts timestamp, c1 binary(128), c2 nchar(128), c3 binary(128), c4 nchar(128)) tags (t1 binary(128), t2 nchar(128), t3 binary(128), t4 nchar(128))"
        )
        tdSql.execute(
            f'create table ctb4 using stb2 tags("tag-binary-4" , "tag-nchar-4", "tag-binary-4" , "tag-nchar-4")'
        )
        tdSql.execute(
            f"create table ntb4 (ts timestamp, c1 binary(128), c2 nchar(128), c3 binary(128), c4 nchar(128))"
        )
        tdSql.execute(
            f'insert into ctb4 values ("2022-01-01 00:00:00.000" , " ab 12 " , " ab 12 " , " cd 34 " , " cd 34 "  )'
        )
        tdSql.execute(
            f'insert into ntb4 values ("2022-01-01 00:00:00.000" , " ab 12 " , " ab 12 " , " cd 34 " , " cd 34 "  )'
        )

        tdSql.execute(
            f'create table ctb5 using stb tags("tag-binary-5" , "tag-nchar-5")'
        )
        tdSql.execute(
            f"create table ntb5 (ts timestamp, c1 binary(128), c2 nchar(128))"
        )
        tdSql.execute(
            f'insert into ctb5 values ("2022-01-01 00:00:00.000" , "0123456789" , "0123456789" )'
        )
        tdSql.execute(
            f'insert into ntb5 values ("2022-01-01 00:00:00.000" , "0123456789" , "0123456789" )'
        )
        tdSql.execute(
            f'insert into ctb5 values ("2022-01-01 00:00:00.001" , NULL , NULL )'
        )
        tdSql.execute(
            f'insert into ntb5 values ("2022-01-01 00:00:00.001" , NULL , NULL )'
        )

        tdSql.execute(
            f"create table stb3 (ts timestamp, c1 binary(64), c2 nchar(64), c3 nchar(64) ) tags (t1 nchar(64))"
        )
        tdSql.execute(f'create table ctb6 using stb3 tags("tag-nchar-6")')
        tdSql.execute(
            f"create table ntb6 (ts timestamp, c1 binary(64), c2 nchar(64), c3 nchar(64) )"
        )
        tdSql.execute(
            f'insert into ctb6 values ("2022-01-01 00:00:00.000" , "0123456789" , "中文测试1" , "中文测试2" )'
        )
        tdSql.execute(
            f'insert into ntb6 values ("2022-01-01 00:00:00.000" , "0123456789" , "中文测试01", "中文测试01" )'
        )
        tdSql.execute(
            f'insert into ctb6 values ("2022-01-01 00:00:00.001" , NULL , NULL, NULL )'
        )
        tdSql.execute(
            f'insert into ntb6 values ("2022-01-01 00:00:00.001" , NULL , NULL, NULL )'
        )

        self.query()

        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        self.query()

    def query(self):

        tdLog.info(f"====> length")
        tdLog.info(f"====> select c1, length(c1), c2, length(c2) from ctb0")
        tdSql.query(f"select c1, length(c1), c2, length(c2) from ctb0")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 11)

        tdSql.checkData(0, 3, 44)

        tdSql.checkData(1, 1, 12)

        tdSql.checkData(1, 3, 48)

        tdLog.info(f"====> select c1, length(c1), c2, length(c2) from ntb0")
        tdSql.query(f"select c1, length(c1), c2, length(c2) from ntb0")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 11)

        tdSql.checkData(0, 3, 44)

        tdSql.checkData(1, 1, 12)

        tdSql.checkData(1, 3, 48)

        tdLog.info(
            f'====> select length("abcd1234"), char_length("abcd1234=-+*") from ntb0'
        )
        tdSql.query(f'select length("abcd1234"), char_length("abcd1234=-+*") from ntb0')
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(4)

        tdSql.checkData(0, 0, 8)

        tdSql.checkData(0, 1, 12)

        tdLog.info(f"====> select c2 ,length(c2), char_length(c2) from ctb6")
        tdSql.query(f"select c2 ,length(c2), char_length(c2) from ctb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 20)

        tdSql.checkData(0, 2, 5)

        tdSql.checkData(1, 1, None)

        tdLog.info(f"====> select c2 ,length(c2),char_length(c2) from ntb6")
        tdSql.query(f"select c2 ,length(c2),char_length(c2) from ntb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 24)

        tdSql.checkData(0, 2, 6)

        tdSql.checkData(1, 1, None)

        tdLog.info(f"====> select c2 ,lower(c2), upper(c2) from ctb6")
        tdSql.query(f"select c2 ,lower(c2), upper(c2) from ctb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, "中文测试1")

        tdSql.checkData(0, 2, "中文测试1")

        tdSql.checkData(1, 1, None)

        tdLog.info(f"====> select c2 ,lower(c2), upper(c2) from ntb6")
        tdSql.query(f"select c2 ,lower(c2), upper(c2) from ntb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(2)

        tdSql.checkData(0, 1, "中文测试01")

        tdSql.checkData(0, 2, "中文测试01")

        tdSql.checkData(1, 1, None)

        tdLog.info(f"====> select c2, ltrim(c2), ltrim(c2) from ctb6")
        tdSql.query(f"select c2, ltrim(c2), ltrim(c2) from ctb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(2)

        tdSql.checkData(0, 1, "中文测试1")

        tdSql.checkData(0, 2, "中文测试1")

        tdSql.checkData(1, 1, None)

        tdLog.info(f"====> select c2, ltrim(c2), ltrim(c2) from ntb6")
        tdSql.query(f"select c2, ltrim(c2), ltrim(c2) from ntb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(
            f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}"
        )
        tdLog.info(
            f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, "中文测试01")

        tdSql.checkData(0, 2, "中文测试01")

        tdSql.checkData(1, 1, None)

        tdLog.info(f"====> select c2, c3 , concat(c2,c3) from ctb6")
        tdSql.query(f"select c2, c3 , concat(c2,c3) from ctb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(2)

        tdSql.checkData(0, 2, "中文测试1中文测试2")

        tdSql.checkData(1, 2, None)

        tdLog.info(f"====> select c2, c3 , concat(c2,c3) from ntb6")
        tdSql.query(f"select c2, c3 , concat(c2,c3) from ntb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(2)

        tdSql.checkData(0, 2, "中文测试01中文测试01")

        tdSql.checkData(1, 2, None)

        tdSql.query(f"select c2, c3 , concat_ws('_', c2, c3) from ctb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(2)

        tdSql.checkData(0, 2, "中文测试1_中文测试2")

        # if $tdSql.getData(1,2) != NULL then
        #   return -1
        # endi

        tdSql.query(f"select c2, c3 , concat_ws('_', c2, c3) from ntb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(2)

        tdSql.checkData(0, 2, "中文测试01_中文测试01")

        # if $tdSql.getData(1,2) != NULL then
        #   return -1
        # endi

        tdLog.info(f"====> select  c2, substr(c2,1, 4) from ctb6")
        tdSql.query(f"select  c2, substr(c2,1, 4) from ctb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "中文测试1")

        tdSql.checkData(0, 1, "中文测试")

        # if $tdSql.getData(1,1) != NULL then
        #   return -1
        # endi

        tdLog.info(f"====> select  c2, substr(c2,1, 4) from ntb6")
        tdSql.query(f"select  c2, substr(c2,1, 4) from ntb6")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        tdLog.info(f"====> {tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"====> {tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "中文测试01")

        tdSql.checkData(0, 1, "中文测试")

        tdSql.checkData(1, 1, None)

        # sql_error select c1, length(t1), c2, length(t2) from ctb0

        tdLog.info(f"====> char_length")
        tdLog.info(f"====> select c1, char_length(c1), c2, char_length(c2) from ctb0")
        tdSql.query(f"select c1, char_length(c1), c2, char_length(c2) from ctb0")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(4)

        tdSql.checkData(2, 1, 12)

        tdSql.checkData(2, 3, 12)

        tdSql.checkData(3, 1, 14)

        tdSql.checkData(3, 3, 14)

        tdLog.info(f"====> select c1, char_length(c1), c2, char_length(c2) from ntb0")
        tdSql.query(f"select c1, char_length(c1), c2, char_length(c2) from ntb0")
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(4)

        tdSql.checkData(2, 1, 12)

        tdSql.checkData(2, 3, 12)

        tdSql.checkData(3, 1, 14)

        tdSql.checkData(3, 3, 14)

        # sql_error select c1, char_length(t1), c2, char_length(t2) from ctb0

        tdLog.info(f"====> lower")
        tdSql.query(
            f'select c1, lower(c1), c2, lower(c2), lower("abcdEFGH=-*&%") from ntb1'
        )
        tdLog.info(
            f'====> select c1, lower(c1), c2, lower(c2), lower("abcdEFGH=-*&%") from ctb1'
        )
        tdSql.query(
            f'select c1, lower(c1), c2, lower(c2), lower("abcdEFGH=-*&%") from ctb1'
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(2)

        tdSql.checkData(0, 1, "abcd1234")

        tdSql.checkData(0, 3, "abcd1234")

        tdSql.checkData(0, 4, "abcdefgh=-*&%")

        tdSql.checkData(1, 1, "aabbccdd1234")

        tdSql.checkData(1, 3, "aabbccdd1234")

        tdSql.checkData(1, 4, "abcdefgh=-*&%")

        # sql_error select c1, lower(t1), c2, lower(t2) from ctb1

        tdLog.info(f"====> upper")
        tdSql.query(
            f'select c1, upper(c1), c2, upper(c2), upper("abcdEFGH=-*&%") from ntb2'
        )
        tdLog.info(
            f'====> select c1, upper(c1), c2, upper(c2), upper("abcdEFGH=-*&%") from ctb2'
        )
        tdSql.query(
            f'select c1, upper(c1), c2, upper(c2), upper("abcdEFGH=-*&%") from ctb2'
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(2)

        tdSql.checkData(0, 1, "ABCD1234")

        tdSql.checkData(0, 3, "ABCD1234")

        tdSql.checkData(0, 4, "ABCDEFGH=-*&%")

        tdSql.checkData(1, 1, "AABBCCDD1234")

        tdSql.checkData(1, 3, "AABBCCDD1234")

        tdSql.checkData(1, 4, "ABCDEFGH=-*&%")

        # sql_error select c1, upper(t1), c2, upper(t2) from ctb2

        tdLog.info(f"====> ltrim")
        tdSql.query(
            f'select c1, ltrim(c1), c2, ltrim(c2), ltrim("  abcdEFGH  =-*&%  ") from ntb3'
        )
        tdLog.info(
            f'====> select c1, ltrim(c1), c2, ltrim(c2), ltrim("  abcdEFGH  =-*&%  ") from ctb3'
        )
        tdSql.query(
            f'select c1, ltrim(c1), c2, ltrim(c2), ltrim("  abcdEFGH  =-*&%  ") from ctb3'
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, "abcd  1234  ")

        tdSql.checkData(0, 3, "abcd  1234  ")

        tdSql.checkData(0, 4, "abcdEFGH  =-*&%  ")

        # sql_error select c1, ltrim(t1), c2, ltrim(t2) from ctb3

        tdLog.info(f"====> rtrim")
        tdSql.query(
            f'select c1, rtrim(c1), c2, rtrim(c2), rtrim("  abcdEFGH  =-*&%  ") from ntb3'
        )
        tdLog.info(
            f'====> select c1, rtrim(c1), c2, rtrim(c2), rtrim("  abcdEFGH  =-*&%  ") from ctb3'
        )
        tdSql.query(
            f'select c1, rtrim(c1), c2, rtrim(c2), rtrim("  abcdEFGH  =-*&%  ") from ctb3'
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, "  abcd  1234")

        tdSql.checkData(0, 3, "  abcd  1234")

        tdSql.checkData(0, 4, "  abcdEFGH  =-*&%")

        # sql_error select c1, rtrim(t1), c2, rtrim(t2) from ctb3

        tdLog.info(f"====> concat")
        tdSql.query(
            f'select c1, c3, concat(c1, c3), c2, c4, concat(c2, c4), concat("binary+", c1, c3), concat("nchar+", c2, c4) from ntb4'
        )
        tdLog.info(
            f'====> select c1, c3, concat(c1, c3), c2, c4, concat(c2, c4), concat("binary+", c1, c3), concat("nchar+", c2, c4) from ctb4'
        )
        tdSql.query(
            f'select c1, c3, concat(c1, c3), c2, c4, concat(c2, c4), concat("binary+", c1, c3), concat("nchar+", c2, c4) from ctb4'
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 2, " ab 12  cd 34 ")

        tdSql.checkData(0, 5, " ab 12  cd 34 ")

        tdSql.checkData(0, 6, "binary+ ab 12  cd 34 ")

        tdSql.checkData(0, 7, "nchar+ ab 12  cd 34 ")

        tdSql.query(
            f'select c1, c3, concat("bin-", c1, "-a1-", "a2-", c3, "-a3-", "a4-", "END"), c2, c4, concat("nchar-", c2, "-a1-", "-a2-", c4, "-a3-", "a4-", "END") from ntb4'
        )
        tdLog.info(
            f'====> select c1, c3, concat("bin-", c1, "-a1-", "a2-", c3, "-a3-", "a4-", "END"), c2, c4, concat("nchar-", c2, "-a1-", "a2-", c4, "-a3-", "a4-", "END") from ctb4'
        )
        tdSql.query(
            f'select c1, c3, concat("bin-", c1, "-a1-", "a2-", c3, "-a3-", "a4-", "END"), c2, c4, concat("nchar-", c2, "-a1-", "a2-", c4, "-a3-", "a4-", "END") from ctb4'
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 2, "bin- ab 12 -a1-a2- cd 34 -a3-a4-END")

        tdSql.checkData(0, 5, "nchar- ab 12 -a1-a2- cd 34 -a3-a4-END")

        # sql_error select c1, c2, concat(c1, c2), c3, c4, concat(c3, c4) from ctb4
        # sql_error select t1, t2, concat(t1, t2), t3, t4, concat(t3, t4) from ctb4
        # sql_error select t1, t3, concat(t1, t3), t2, t4, concat(t2, t4) from ctb4

        tdLog.info(f"====> concat_ws")
        tdSql.query(
            f'select c1, c3, concat_ws("*", c1, c3), c2, c4, concat_ws("*", c2, c4), concat_ws("*", "binary+", c1, c3), concat_ws("*", "nchar+", c2, c4) from ntb4'
        )
        tdLog.info(
            f'====> select c1, c3, concat_ws("*", c1, c3), c2, c4, concat_ws("*", c2, c4), concat_ws("*", "binary+", c1, c3), concat_ws("*", "nchar+", c2, c4) from ctb4'
        )
        tdSql.query(
            f'select c1, c3, concat_ws("*", c1, c3), c2, c4, concat_ws("*", c2, c4), concat_ws("*", "binary+", c1, c3), concat_ws("*", "nchar+", c2, c4) from ctb4'
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 2, " ab 12 * cd 34 ")

        tdSql.checkData(0, 5, " ab 12 * cd 34 ")

        tdSql.checkData(0, 6, "binary+* ab 12 * cd 34 ")

        tdSql.checkData(0, 7, "nchar+* ab 12 * cd 34 ")

        tdLog.info(
            f'====> select c1, c3, concat_ws("*", "b0", c1, "b1", c3, "b2", "E0", "E1", "E2"), c2, c4, concat_ws("*", "n0", c2, c4, "n1", c2, c4, "n2", "END") from ctb4'
        )
        tdSql.query(
            f'select c1, c3, concat_ws("*", "b0", c1, "b1", c3, "b2", "E0", "E1", "E2"), c2, c4, concat_ws("*", "n0", c2, c4, "n1", c2, c4, "n2", "END") from ctb4'
        )
        tdLog.info(f"====> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)

        tdSql.checkData(0, 2, "b0* ab 12 *b1* cd 34 *b2*E0*E1*E2")

        tdSql.checkData(0, 5, "n0* ab 12 * cd 34 *n1* ab 12 * cd 34 *n2*END")

        # sql_error select c1, c2, concat_ws("*", c1, c2), c3, c4, concat_ws("*", c3, c4) from ctb4
        # sql_error select t1, t2, concat_ws("*", t1, t2), t3, t4, concat_ws("*", t3, t4) from ctb4
        # sql_error select t1, t3, concat_ws("*", t1, t3), t2, t4, concat_ws("*", t2, t4) from ctb4

        tdLog.info(f"====> substr")


# sql select c1, substr(c1, 3, 3), substr(c1, -5, 3), c2, substr(c2, 3, 3), substr(c2, -5, 3), substr("abcdefg", 3, 3), substr("abcdefg", -3, 3) from ntb5
# print ====> select c1, substr(c1, 3, 3), substr(c1, -5, 3), c2, substr(c2, 3, 3), substr(c2, -5, 3), substr("abcdefg", 3, 3), substr("abcdefg", -3, 3) from ctb5
# sql select c1, substr(c1, 3, 3), substr(c1, -5, 3), c2, substr(c2, 3, 3), substr(c2, -5, 3), substr("abcdefg", 3, 3), substr("abcdefg", -3, 3) from ctb5
# print ====> rows: $rows
# print ====> $tdSql.getData(0,0) $tdSql.getData(0,1) $tdSql.getData(0,2) $tdSql.getData(0,3) $tdSql.getData(0,4) $tdSql.getData(0,5) $tdSql.getData(0,6)
# print ====> $tdSql.getData(1,0) $tdSql.getData(1,1) $tdSql.getData(1,2) $tdSql.getData(1,3) $tdSql.getData(1,4) $tdSql.getData(1,5) $tdSql.getData(1,6)
# if $rows != 1 then
#  return -1
# endi
# if $tdSql.getData(0,1) != 345 then
#  return -1
# endi
# if $tdSql.getData(0,2) != 456 then
#  return -1
# endi
# if $tdSql.getData(0,4) != 345 then
#  return -1
# endi
# if $tdSql.getData(0,5) != 456 then
#  return -1
# endi
# if $tdSql.getData(0,6) != def then
#  return -1
# endi
# if $tdSql.getData(0,7) != efg then
#  return -1
# endi
# if $tdSql.getData(1,1) != NULL then
#  return -1
# endi
# if $tdSql.getData(1,2) != NULL then
#  return -1
# endi
# if $tdSql.getData(1,4) != NULL then
#  return -1
# endi
# if $tdSql.getData(1,5) != NULL then
#  return -1
# endi
# if $tdSql.getData(1,6) != def then
#  return -1
# endi
# if $tdSql.getData(1,7) != efg then
#  return -1
# endi
#
# sql select c1, substr(c1, 3), substr(c1, -5), c2, substr(c2, 3), substr(c2, -5), substr("abcdefg", 3), substr("abcdefg", -3) from ntb5
# print ====> select c1, substr(c1, 3), substr(c1, -5), c2, substr(c2, 3), substr(c2, -5), substr("abcdefg", 3), substr("abcdefg", -3) from ctb5
# sql select c1, substr(c1, 3), substr(c1, -5), c2, substr(c2, 3), substr(c2, -5), substr("abcdefg", 3), substr("abcdefg", -3) from ctb5
# print ====> rows: $rows
# print ====> $tdSql.getData(0,0) $tdSql.getData(0,1) $tdSql.getData(0,2) $tdSql.getData(0,3) $tdSql.getData(0,4) $tdSql.getData(0,5) $tdSql.getData(0,6)
# print ====> $tdSql.getData(1,0) $tdSql.getData(1,1) $tdSql.getData(1,2) $tdSql.getData(1,3) $tdSql.getData(1,4) $tdSql.getData(1,5) $tdSql.getData(1,6)
# if $rows != 1 then
#  return -1
# endi
# if $tdSql.getData(0,1) != 3456789 then
#  return -1
# endi
# if $tdSql.getData(0,2) != 456789 then
#  return -1
# endi
# if $tdSql.getData(0,4) != 3456789 then
#  return -1
# endi
# if $tdSql.getData(0,5) != 456789 then
#  return -1
# endi
# if $tdSql.getData(0,6) != defg then
#  return -1
# endi
# if $tdSql.getData(0,7) != efg then
#  return -1
# endi
# if $tdSql.getData(1,1) != NULL then
#  return -1
# endi
# if $tdSql.getData(1,2) != NULL then
#  return -1
# endi
# if $tdSql.getData(1,4) != NULL then
#  return -1
# endi
# if $tdSql.getData(1,5) != NULL then
#  return -1
# endi
# if $tdSql.getData(1,6) != defg then
#  return -1
# endi
# if $tdSql.getData(1,7) != efg then
#  return -1
# endi

# sql_error select t1, substr(t1, 3, 2), substr(t1, -3, 2), t2, substr(t2, 3, 2), substr(t2, -3, 2) from ctb5
