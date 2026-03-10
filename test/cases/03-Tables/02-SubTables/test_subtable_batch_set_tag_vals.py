from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSubTableBatchSetTagVals:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ==================== multi_table batch tests ====================

    def multi_table_basic(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int, t2 binary(10))")
        for i in range(10):
            tdSql.execute(f"create table ct{i} using stb tags({i}, 'tag{i}')")
            tdSql.execute(f"insert into ct{i} values(now, {i*10})")

        # update one tag of one table
        tdSql.execute("alter table ct1 set tag t1 = 111")
        tdSql.query("select tbname, t1 from stb where tbname='ct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 111)

        # update multiple tags of one table
        tdSql.execute("alter table ct2 set tag t1=222, t2='updated'")
        tdSql.query("select tbname, t1, t2 from stb where tbname='ct2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 222)
        tdSql.checkData(0, 2, "updated")

        # update multiple tags of multiple tables
        sql = "alter table "
        for i in range(10):
            sql += f"ct{i} set tag t1={i*10}, t2='updated{i}' "
        tdSql.execute(sql)
        tdSql.query("select tbname, t1, t2 from stb order by t1")
        tdSql.checkRows(10)
        for i in range(10):
            tdSql.checkData(i, 1, i*10)
            tdSql.checkData(i, 2, f"updated{i}")

        # set tag value to null
        tdSql.execute("alter table ct3 set tag t1=NULL ct4 set tag t1 = NULL, t2=NULL")
        tdSql.query("select tbname, t1, t2 from stb where tbname='ct3'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, "updated3")
        tdSql.query("select tbname, t1, t2 from stb where tbname='ct4'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

    def multi_table_various_types(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        tdSql.execute(
            "create table stb (ts timestamp, c1 int) "
            "tags(t_bool bool, t_tinyint tinyint, t_smallint smallint, t_int int, "
            "t_bigint bigint, t_float float, t_double double, "
            "t_binary binary(30), t_nchar nchar(30))"
        )
        tdSql.execute(
            "create table ct1 using stb tags(true, 1, 1, 1, 1, 1.0, 1.0, 'bin1', 'nch1')"
        )
        tdSql.execute(
            "create table ct2 using stb tags(false, 2, 2, 2, 2, 2.0, 2.0, 'bin2', 'nch2')"
        )
        tdSql.execute("insert into ct1 values(now, 10)")
        tdSql.execute("insert into ct2 values(now, 20)")

        # batch set all tag types on both tables
        tdSql.execute(
            "alter table "
            "ct1 set tag t_bool=false, t_tinyint=10, t_smallint=100, t_int=1000, "
            "t_bigint=10000, t_float=1.5, t_double=2.5, t_binary='updated1', t_nchar='更新1' "
            "ct2 set tag t_bool=true, t_tinyint=-1, t_smallint=-10, t_int=-100, "
            "t_bigint=-1000, t_float=-1.5, t_double=-2.5, t_binary='updated2', t_nchar='更新2'"
        )
        tdSql.execute("reset query cache")

        # verify ct1
        tdSql.query("select * from stb where tbname='ct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 0)       # t_bool = false
        tdSql.checkData(0, 3, 10)      # t_tinyint
        tdSql.checkData(0, 4, 100)     # t_smallint
        tdSql.checkData(0, 5, 1000)    # t_int
        tdSql.checkData(0, 6, 10000)   # t_bigint
        tdSql.checkData(0, 9, "updated1")
        tdSql.checkData(0, 10, "更新1")

        # verify ct2
        tdSql.query("select * from stb where tbname='ct2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 1)       # t_bool = true
        tdSql.checkData(0, 3, -1)      # t_tinyint
        tdSql.checkData(0, 4, -10)     # t_smallint
        tdSql.checkData(0, 5, -100)    # t_int
        tdSql.checkData(0, 6, -1000)   # t_bigint
        tdSql.checkData(0, 9, "updated2")
        tdSql.checkData(0, 10, "更新2")

    def multi_table_error_cases(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        tdSql.execute(
            "create table stb (ts timestamp, c1 int) tags(t1 int, t2 binary(10))"
        )
        tdSql.execute("create table ct1 using stb tags(1, 'a')")
        tdSql.execute("create table ct2 using stb tags(2, 'b')")
        tdSql.execute("create table ntb (ts timestamp, c1 int)")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("insert into ct2 values(now, 2)")

        # alter tag of super table
        tdSql.error("alter table stb set tag t1=1")
        # duplicate tag in same table clause
        tdSql.error("alter table ct1 set tag t1=1, t1=2")
        # non-existent tag
        tdSql.error("alter table ct1 set tag t_notexist=1 ct2 set tag t1=2")
        # set tag on supertable (not child table)
        tdSql.error("alter table stb set tag t1=1 ct2 set tag t1=2")
        # set tag on normal table
        tdSql.error("alter table ntb set tag t1=1 ct2 set tag t1=2")
        # non-existent table
        tdSql.error("alter table ct_notexist set tag t1=1 ct2 set tag t1=2")
        # wrong data type
        tdSql.error("alter table ct1 set tag t1='string not int' ct2 set tag t1=2.5")

        # max/min length for binary tag should succeed
        tdSql.error("alter table ct1 set tag t2='a long str.' ct2 set tag t2='also too long'")
        tdSql.execute("alter table ct1 set tag t2='1234567890' ct2 set tag t2=''")
        tdSql.query("select tbname, t2 from stb where tbname='ct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "1234567890")
        tdSql.query("select tbname, t2 from stb where tbname='ct2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "")


    def test_multi_table_batch_set_tag(self):
        """Batch set tag values on multiple subtables in one ALTER TABLE statement

        ALTER TABLE
            subtable1 SET TAG tag1=value1, tag2=value2, ...
            subtable2 SET TAG tag1=value3, tag2=value4, ...

        Catalog:
            - Table:SubTable

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-11 Bomin Zhang created.
        """

        self.multi_table_basic()
        self.multi_table_various_types()
        self.multi_table_error_cases()


    # ==================== USING stable SET TAG ... WHERE tests ====================

    def child_table_basic(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int, t2 binary(20))")
        for i in range(100):
            tdSql.execute(f"create table ct{i} using stb tags({i}, 'tag{i}')")
            tdSql.execute(f"insert into ct{i} values(now, {i*10})")

        # no matches for WHERE condition should succeed without error, no update
        tdSql.execute("alter table using stb set tag t2='updated2' where t1=3000")
        tdSql.query("select tbname, t2 from stb order by t1")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, f'tag{i}')

        # update tag value to NULL
        tdSql.execute("alter table using stb set tag t1 = NULL where t1 < 10")
        for i in range(100):
            tdSql.query(f"select tbname, t1 from stb where tbname='ct{i}'")
            tdSql.checkRows(1)
            if i < 10:
                tdSql.checkData(0, 1, None)
            else:
                tdSql.checkData(0, 1, i)

        # update tag on child table without a WHERE condition,
        # all child tables should be updated with the same value
        tdSql.execute("alter table using stb set tag t1=1000")
        tdSql.query("select tbname, t1 from stb order by tbname")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, 1000)

        # update tag on child table matching WHERE tbname
        tdSql.execute("alter table using stb set tag t1=2000 where tbname='ct1'")
        tdSql.query("select tbname, t1, t2 from stb where tbname='ct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(0, 2, "tag1")

        # other tables should not be changed
        tdSql.query("select tbname, t1 from stb where tbname<>'ct1'")
        tdSql.checkRows(99)
        for i in range(99):
            tdSql.checkData(i, 1, 1000)
        
        # update tag t2 on child tables matching WHERE t1=1000
        tdSql.execute("alter table using stb set tag t2='updated' where t1=1000")
        tdSql.query("select tbname, t1, t2 from stb where t1=1000 order by tbname")
        tdSql.checkRows(99)
        for i in range(99):
            tdSql.checkData(i, 1, 1000)
            tdSql.checkData(i, 2, "updated")
        
        # child table ct1 (t1=2000) should be unchanged
        tdSql.query("select tbname, t1, t2 from stb where tbname='ct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(0, 2, "tag1")


    def child_table_regexp_replace(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int, t2 binary(30), t3 binary(30), t4 nchar(20))")
        tdSql.execute("create table ct1 using stb tags(1, 'wangjing.beijing.china', '望京.北京.中国', '望京.北京.中国')")
        tdSql.execute("insert into ct1 values(now, 10)")
        tdSql.execute("create table ct2 using stb tags(2, 'heping.tianjin.china', '和平.天津.中国', '和平.天津.中国')")
        tdSql.execute("insert into ct2 values(now, 20)")
        tdSql.execute("create table ct3 using stb tags(3, 'xuhui.shanghai.china', '徐汇.上海.中国', '徐汇.上海.中国')")
        tdSql.execute("insert into ct3 values(now, 30)")
        tdSql.execute("create table ct4 using stb tags(4, null, null, null)")
        tdSql.execute("insert into ct4 values(now, 40)")

        # replace all city to beijing
        tdSql.execute("alter table using stb set tag " +
                      "t2=REGEXP_REPLACE(t2, '^([^.]+\\.)[^.]+(\\.china)$', '$1beijing$2')," + 
                      "t3=REGEXP_REPLACE(t3, '^([^.]+\\.)[^.]+(\\.中国)$', '$1北京$2'), " +
                      "t4=REGEXP_REPLACE(t4, '^([^.]+\\.)[^.]+(\\.中国)$', '$1北京$2') " +
                      "where t1 <> 1")
        tdSql.query("select tbname, t2, t3, t4 from stb order by t1")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, "wangjing.beijing.china")
        tdSql.checkData(0, 2, "望京.北京.中国")
        tdSql.checkData(0, 3, "望京.北京.中国")
        tdSql.checkData(1, 1, "heping.beijing.china")
        tdSql.checkData(1, 2, "和平.北京.中国")
        tdSql.checkData(1, 3, "和平.北京.中国")
        tdSql.checkData(2, 1, "xuhui.beijing.china")
        tdSql.checkData(2, 2, "徐汇.北京.中国")
        tdSql.checkData(2, 3, "徐汇.北京.中国")
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 3, None)

        # replace result is too long for binary(30) tag, should error
        tdSql.error("alter table using stb set tag t3=REGEXP_REPLACE(t3, '^([^.]+\\.[^.]+\\.)中国$', '$1中华人民共和国') where t1 = 1")
        # but should succeed for nchar(20) tag
        tdSql.execute("alter table using stb set tag t4=REGEXP_REPLACE(t4, '^([^.]+\\.[^.]+\\.)中国$', '$1中华人民共和国') where t1 = 1")
        tdSql.query("select tbname, t3, t4 from stb where t1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "望京.北京.中国")
        tdSql.checkData(0, 2, "望京.北京.中华人民共和国")

        # value should not be changed if regex does not match
        tdSql.execute("alter table using stb set tag t2=REGEXP_REPLACE(t2, '^([^.]+\\.)[^.]+(\\.america)$', '$1shanghai$2') where t1 = 1")
        tdSql.query("select tbname, t2 from stb where t1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "wangjing.beijing.china")


    def child_table_error_cases(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        tdSql.execute("create table stb (ts timestamp, c1 int) tags(t1 int, t2 binary(10))")
        for i in range(10):
            tdSql.execute(f"create table ct{i} using stb tags({i}, 'tag{i}')")
            tdSql.execute(f"insert into ct{i} values(now, {i*10})")
        tdSql.execute("create table ntb (ts timestamp, c1 int)")

        # non-existent supertable
        tdSql.error("alter table using stb_notexist set tag t1=1 where t1=1")
        # non-existent tag name
        tdSql.error("alter table using stb set tag t_notexist=1 where t1=1")
        # non-string tag in regex replace
        tdSql.error("alter table using stb set tag t1=REGEXP_REPLACE(t1, '[0-9]+', 'replacement') where t1=1")
        # duplicate tag in list
        tdSql.error("alter table using stb set tag t1=1, t1=2 where t1=1")
        # invalid regex in tag value expression
        tdSql.error("alter table using stb set tag t2=REGEXP_REPLACE(t2, '[invalid', 'replacement') where t1=1")
        # normal table used in USING
        tdSql.error("alter table using ntb set tag t1=1 where t1=1")
        # using aggregate function in where clause is not allowed
        tdSql.error("alter table using stb set tag t1=1 where MAX(t1)=1")


    def test_child_table_batch_set_tag(self):
        """Set tag values on child tables via USING supertable with optional WHERE clause

        ALTER TABLE USING supertable SET TAG tag1=value1, tag2=value2, ...  WHERE condition

        Catalog:
            - Table:SubTable

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-11 Bomin Zhang created.
        """

        self.child_table_basic()
        self.child_table_regexp_replace()
        self.child_table_error_cases()
