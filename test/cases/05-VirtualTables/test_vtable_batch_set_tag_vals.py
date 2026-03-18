from new_test_framework.utils import tdLog, tdSql


class TestVtableBatchSetTagVals:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ==================== multi_table batch tests ====================

    def multi_table_basic(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        # create source tables
        tdSql.execute("create table org_tb (ts timestamp, c1 int)")
        tdSql.execute("insert into org_tb values(now, 1)")

        # create virtual super table and virtual child tables
        tdSql.execute("create stable vstb (ts timestamp, c1 int) tags(t1 int, t2 binary(10)) virtual 1")
        for i in range(10):
            tdSql.execute(
                f"create vtable vct{i} (c1 from org_tb.c1) using vstb tags({i}, 'tag{i}')"
            )

        # update one tag of one virtual child table
        tdSql.execute("alter vtable vct1 set tag t1 = 111")
        tdSql.query("select tbname, t1 from vstb where tbname='vct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 111)

        # update multiple tags of one virtual child table
        tdSql.execute("alter vtable vct2 set tag t1=222, t2='updated'")
        tdSql.query("select tbname, t1, t2 from vstb where tbname='vct2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 222)
        tdSql.checkData(0, 2, "updated")

        # update multiple tags of multiple virtual child tables
        sql = "alter vtable "
        for i in range(10):
            sql += f"vct{i} set tag t1={i*10}, t2='updated{i}' "
        tdSql.execute(sql)
        tdSql.query("select tbname, t1, t2 from vstb order by t1")
        tdSql.checkRows(10)
        for i in range(10):
            tdSql.checkData(i, 1, i * 10)
            tdSql.checkData(i, 2, f"updated{i}")

        # set tag value to null
        tdSql.execute("alter vtable vct3 set tag t1=NULL vct4 set tag t1 = NULL, t2=NULL")
        tdSql.query("select tbname, t1, t2 from vstb where tbname='vct3'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, "updated3")
        tdSql.query("select tbname, t1, t2 from vstb where tbname='vct4'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

    def multi_table_across_database(self):
        tdSql.execute("drop database if exists test1")
        tdSql.execute("drop database if exists test2")
        tdSql.execute("create database test1 vgroups 1")
        tdSql.execute("create database test2 vgroups 1")

        # create source and virtual table in test1
        tdSql.execute("use test1")
        tdSql.execute("create table org_tb (ts timestamp, c1 int)")
        tdSql.execute("insert into org_tb values(now, 1)")
        tdSql.execute("create stable vstb (ts timestamp, c1 int) tags(t1 int, t2 varchar(10)) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from org_tb.c1) using vstb tags(1, 'tag1')")

        # create source and virtual table in test2
        tdSql.execute("use test2")
        tdSql.execute("create table org_tb (ts timestamp, c1 int)")
        tdSql.execute("insert into org_tb values(now, 1)")
        tdSql.execute("create stable vstb (ts timestamp, c1 int) tags(t1 int, t2 varchar(10)) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from org_tb.c1) using vstb tags(2, 'tag2')")

        # batch set tags on virtual child tables in different databases
        tdSql.execute(
            "alter vtable test1.vct1 set tag t1=111, t2='updated1' "
            "test2.vct1 set tag t1=222, t2='updated2'"
        )
        tdSql.query("select tbname, t1, t2 from test1.vstb where tbname='vct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 111)
        tdSql.checkData(0, 2, "updated1")
        tdSql.query("select tbname, t1, t2 from test2.vstb where tbname='vct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 222)
        tdSql.checkData(0, 2, "updated2")


    def multi_table_various_types(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        # create source table
        tdSql.execute("create table org_tb (ts timestamp, c1 int)")
        tdSql.execute("insert into org_tb values(now, 10)")

        tdSql.execute(
            "create stable vstb (ts timestamp, c1 int) "
            "tags(t_bool bool, t_tinyint tinyint, t_smallint smallint, t_int int, "
            "t_bigint bigint, t_float float, t_double double, "
            "t_binary binary(30), t_nchar nchar(30)) virtual 1"
        )
        tdSql.execute(
            "create vtable vct1 (c1 from org_tb.c1) "
            "using vstb tags(true, 1, 1, 1, 1, 1.0, 1.0, 'bin1', 'nch1')"
        )
        tdSql.execute(
            "create vtable vct2 (c1 from org_tb.c1) "
            "using vstb tags(false, 2, 2, 2, 2, 2.0, 2.0, 'bin2', 'nch2')"
        )

        # batch set all tag types on both virtual child tables
        tdSql.execute(
            "alter vtable "
            "vct1 set tag t_bool=false, t_tinyint=10, t_smallint=100, t_int=1000, "
            "t_bigint=10000, t_float=1.5, t_double=2.5, t_binary='updated1', t_nchar='更新1' "
            "vct2 set tag t_bool=true, t_tinyint=-1, t_smallint=-10, t_int=-100, "
            "t_bigint=-1000, t_float=-1.5, t_double=-2.5, t_binary='updated2', t_nchar='更新2'"
        )
        tdSql.execute("reset query cache")

        # verify vct1
        tdSql.query("select * from vstb where tbname='vct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 0)       # t_bool = false
        tdSql.checkData(0, 3, 10)      # t_tinyint
        tdSql.checkData(0, 4, 100)     # t_smallint
        tdSql.checkData(0, 5, 1000)    # t_int
        tdSql.checkData(0, 6, 10000)   # t_bigint
        tdSql.checkData(0, 9, "updated1")
        tdSql.checkData(0, 10, "更新1")

        # verify vct2
        tdSql.query("select * from vstb where tbname='vct2'")
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

        # create source table
        tdSql.execute("create table org_tb (ts timestamp, c1 int)")
        tdSql.execute("insert into org_tb values(now, 1)")

        tdSql.execute(
            "create stable vstb (ts timestamp, c1 int) tags(t1 int, t2 binary(10)) virtual 1"
        )
        tdSql.execute("create vtable vct1 (c1 from org_tb.c1) using vstb tags(1, 'a')")
        tdSql.execute("create vtable vct2 (c1 from org_tb.c1) using vstb tags(2, 'b')")
        # create a virtual normal table (no tags)
        tdSql.execute("create vtable vntb (ts timestamp, c1 int from org_tb.c1)")

        # alter tag of virtual super table
        tdSql.error("alter vtable vstb set tag t1=1")
        # duplicate tag in same table clause
        tdSql.error("alter vtable vct1 set tag t1=1, t1=2")
        # non-existent tag
        tdSql.error("alter vtable vct1 set tag t_notexist=1 vct2 set tag t1=2")
        # set tag on virtual super table (not child table)
        tdSql.error("alter vtable vstb set tag t1=1 vct2 set tag t1=2")
        # set tag on virtual normal table (has no tags)
        tdSql.error("alter vtable vntb set tag t1=1 vct2 set tag t1=2")
        # non-existent table
        tdSql.error("alter vtable vct_notexist set tag t1=1 vct2 set tag t1=2")
        # wrong data type
        tdSql.error("alter vtable vct1 set tag t1='string not int' vct2 set tag t1=2.5")

        # max/min length for binary tag
        tdSql.error("alter vtable vct1 set tag t2='a long str.' vct2 set tag t2='also too long'")
        tdSql.execute("alter vtable vct1 set tag t2='1234567890' vct2 set tag t2=''")
        tdSql.query("select tbname, t2 from vstb where tbname='vct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "1234567890")
        tdSql.query("select tbname, t2 from vstb where tbname='vct2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "")

    def test_multi_table_batch_set_tag(self):
        """Batch set tag values on multiple virtual child tables in one ALTER VTABLE statement

        ALTER VTABLE
            vtable1 SET TAG tag1=value1, tag2=value2, ...
            vtable2 SET TAG tag1=value3, tag2=value4, ...

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-11 Bomin Zhang created.
        """

        self.multi_table_basic()
        self.multi_table_various_types()
        self.multi_table_across_database()
        self.multi_table_error_cases()

    # ==================== USING vstable SET TAG ... WHERE tests ====================

    def child_table_basic(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        # create source table
        tdSql.execute("create table org_tb (ts timestamp, c1 int)")
        tdSql.execute("insert into org_tb values(now, 1)")

        tdSql.execute("create stable vstb (ts timestamp, c1 int) tags(t1 int, t2 binary(20)) virtual 1")
        for i in range(100):
            tdSql.execute(
                f"create vtable vct{i} (c1 from org_tb.c1) using vstb tags({i}, 'tag{i}')"
            )

        # no matches for WHERE condition should succeed without error, no update
        tdSql.execute("alter vtable using vstb set tag t2='updated2' where t1=3000")
        tdSql.query("select tbname, t2 from vstb order by t1")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, f'tag{i}')

        # update tag value to NULL
        tdSql.execute("alter vtable using vstb set tag t1 = NULL where t1 < 10")
        for i in range(100):
            tdSql.query(f"select tbname, t1 from vstb where tbname='vct{i}'")
            tdSql.checkRows(1)
            if i < 10:
                tdSql.checkData(0, 1, None)
            else:
                tdSql.checkData(0, 1, i)

        # update tag on virtual child table without a WHERE condition,
        # all virtual child tables should be updated with the same value
        tdSql.execute("alter vtable using vstb set tag t1=1000")
        tdSql.query("select tbname, t1 from vstb order by tbname")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, 1000)

        # update tag on virtual child table matching WHERE tbname
        tdSql.execute("alter vtable using vstb set tag t1=2000 where tbname='vct1'")
        tdSql.query("select tbname, t1, t2 from vstb where tbname='vct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(0, 2, "tag1")

        # other tables should not be changed
        tdSql.query("select tbname, t1 from vstb where tbname<>'vct1'")
        tdSql.checkRows(99)
        for i in range(99):
            tdSql.checkData(i, 1, 1000)

        # update tag t2 on virtual child tables matching WHERE t1=1000
        tdSql.execute("alter vtable using vstb set tag t2='updated' where t1=1000")
        tdSql.query("select tbname, t1, t2 from vstb where t1=1000 order by tbname")
        tdSql.checkRows(99)
        for i in range(99):
            tdSql.checkData(i, 1, 1000)
            tdSql.checkData(i, 2, "updated")

        # virtual child table vct1 (t1=2000) should be unchanged
        tdSql.query("select tbname, t1, t2 from vstb where tbname='vct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(0, 2, "tag1")

    def child_table_regexp_replace(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        # create source table
        tdSql.execute("create table org_tb (ts timestamp, c1 int)")
        tdSql.execute("insert into org_tb values(now, 1)")

        tdSql.execute("create stable vstb (ts timestamp, c1 int) tags(t1 int, t2 binary(30), t3 binary(30), t4 nchar(20)) virtual 1")
        tdSql.execute("create vtable vct1 (c1 from org_tb.c1) using vstb tags(1, 'wangjing.beijing.china', '望京.北京.中国', '望京.北京.中国')")
        tdSql.execute("create vtable vct2 (c1 from org_tb.c1) using vstb tags(2, 'heping.tianjin.china', '和平.天津.中国', '和平.天津.中国')")
        tdSql.execute("create vtable vct3 (c1 from org_tb.c1) using vstb tags(3, 'xuhui.shanghai.china', '徐汇.上海.中国', '徐汇.上海.中国')")
        tdSql.execute("create vtable vct4 (c1 from org_tb.c1) using vstb tags(4, null, null, null)")

        # replace all city to beijing
        tdSql.execute("alter vtable using vstb set tag " +
                      "t2=REGEXP_REPLACE(t2, '^([^.]+\\.)[^.]+(\\.china)$', '$1beijing$2')," +
                      "t3=REGEXP_REPLACE(t3, '^([^.]+\\.)[^.]+(\\.中国)$', '$1北京$2'), " +
                      "t4=REGEXP_REPLACE(t4, '^([^.]+\\.)[^.]+(\\.中国)$', '$1北京$2') " +
                      "where t1 <> 1")
        tdSql.query("select tbname, t2, t3, t4 from vstb order by t1")
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
        tdSql.error("alter vtable using vstb set tag t3=REGEXP_REPLACE(t3, '^([^.]+\\.[^.]+\\.)中国$', '$1中华人民共和国') where t1 = 1")
        # but should succeed for nchar(20) tag
        tdSql.execute("alter vtable using vstb set tag t4=REGEXP_REPLACE(t4, '^([^.]+\\.[^.]+\\.)中国$', '$1中华人民共和国') where t1 = 1")
        tdSql.query("select tbname, t3, t4 from vstb where t1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "望京.北京.中国")
        tdSql.checkData(0, 2, "望京.北京.中华人民共和国")

        # value should not be changed if regex does not match
        tdSql.execute("alter vtable using vstb set tag t2=REGEXP_REPLACE(t2, '^([^.]+\\.)[^.]+(\\.america)$', '$1shanghai$2') where t1 = 1")
        tdSql.query("select tbname, t2 from vstb where t1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "wangjing.beijing.china")

    def child_table_error_cases(self):
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test vgroups 4")
        tdSql.execute("use test")

        # create source table
        tdSql.execute("create table org_tb (ts timestamp, c1 int)")
        tdSql.execute("insert into org_tb values(now, 1)")

        tdSql.execute("create stable vstb (ts timestamp, c1 int) tags(t1 int, t2 binary(10)) virtual 1")
        for i in range(10):
            tdSql.execute(f"create vtable vct{i} (c1 from org_tb.c1) using vstb tags({i}, 'tag{i}')")
        # create a virtual normal table (no tags)
        tdSql.execute("create vtable vntb (ts timestamp, c1 int from org_tb.c1)")

        # non-existent virtual super table
        tdSql.error("alter vtable using vstb_notexist set tag t1=1 where t1=1")
        # non-existent tag name
        tdSql.error("alter vtable using vstb set tag t_notexist=1 where t1=1")
        # non-string tag in regex replace
        tdSql.error("alter vtable using vstb set tag t1=REGEXP_REPLACE(t1, '[0-9]+', 'replacement') where t1=1")
        # duplicate tag in list
        tdSql.error("alter vtable using vstb set tag t1=1, t1=2 where t1=1")
        # invalid regex in tag value expression
        tdSql.error("alter vtable using vstb set tag t2=REGEXP_REPLACE(t2, '[invalid', 'replacement') where t1=1")
        # normal virtual table used in USING
        tdSql.error("alter vtable using vntb set tag t1=1 where t1=1")
        # using aggregate function in where clause is not allowed
        tdSql.error("alter vtable using vstb set tag t1=1 where MAX(t1)=1")
        # using non-tag column in where clause is not allowed
        tdSql.error("alter vtable using vstb set tag t1=1 where c1=10")


    def test_child_table_batch_set_tag(self):
        """Set tag values on virtual child tables via USING vstable with optional WHERE clause

        ALTER VTABLE USING vstable SET TAG tag1=value1, tag2=value2, ...  WHERE condition

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-11 Bomin Zhang created.
        """

        self.child_table_basic()
        self.child_table_regexp_replace()
        self.child_table_error_cases()
