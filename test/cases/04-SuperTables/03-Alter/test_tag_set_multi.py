from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagChangeMulti:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_change_multi(self):
        """同时修改多个标签值

        1. 创建包含多个标签的超级表
        2. 创建子表并写入数据
        3. 同时修改多个标签的值，确认生效
        4. 使用修改后的标签名进行查询

        Catalog:
            - SuperTable:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/tag/change_multi_tag.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_ad_db"
        tbPrefix = "ta_ad_tb"
        mtPrefix = "ta_ad_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        j = 3
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tbj = tbPrefix + str(j)
        ntable = "tb_normal_table"

        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tagCol1 bool, tagCol2 tinyint, tagCol3 smallint, tagCol4 int, tagCol5 bigint, tagCol6 nchar(10), tagCol7 binary(8))"
        )
        tdSql.execute(
            f'create table {tb} using {mt} tags( 1, 2, 3, 5,7, "test", "test")'
        )
        tdSql.execute(
            f'create table {tbj} using {mt} tags( 2, 3, 4, 6,8, "testj", "testj")'
        )
        tdSql.execute(f"create table {ntable} (ts timestamp, f int)")

        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.execute(f"insert into {tb} values(now, 1)")

        # invalid sql
        tdSql.error(f" alter table {mt} set tag tgcol1 = 1,")
        tdSql.error(f" alter table {mt} set tag ,")
        tdSql.error(f" alter table {mt} set tag tgcol1=10,tagcol2=")
        # set tag value on supertable
        tdSql.error(f"alter table {mt} set tag tgcol1 = 1,tagcol2 = 2, tag3 = 4")
        # set normal table value
        tdSql.error(f"alter table {ntable} set tag f = 10")
        # duplicate tag name
        tdSql.error(f"alter table {tbj} set tag tagCol1=1,tagCol1 = 2")
        tdSql.error(f"alter table {tbj} set tag tagCol1=1,tagCol5=10, tagCol5=3")
        # not exist tag
        tdSql.error(f"alter table {tbj} set tag tagNotExist = 1,tagCol1 = 2")
        tdSql.error(f"alter table {tbj} set tag tagCol1 = 2, tagNotExist = 1")
        tdSql.error(f"alter table {tbj} set tagNotExist = 1")
        tdSql.error(f"alter table {tbj} set tagNotExist = NULL,")
        tdSql.error(
            f'alter table {tbj} set tag tagCol1 = 1, tagCol5="xxxxxxxxxxxxxxxx"'
        )

        tdSql.error(
            f'alter table {tbj} set tag tagCol1 = 1, tagCol5="xxxxxxxxxxxxxxxx", tagCol7="yyyyyyyyyyyyyyyyyyyyyyyyy"'
        )
        # invalid data type

        # escape
        tdSql.error(f"alter table {tbj} set tag `tagCol1`=true")
        tdSql.error(
            f"alter table {tbj} set tag `tagCol1`=true,`tagCol2`=1,`tagNotExist`=10"
        )
        tdSql.error(
            f"alter table {tbj} set tag `tagCol1`=true,`tagCol2`=1,tagcol1=true"
        )

        tdSql.execute(f"alter table {tbj} set tag tagCol1 = 100, tagCol2 = 100")

        tdSql.query(f"select * from {mt} where tagCol2 = 100")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol1 = 1")
        tdSql.checkRows(2)

        tdSql.execute(
            f'alter table {tbj} set tag tagCol1=true,tagCol2=-1,tagcol3=-10, tagcol4=-100,tagcol5=-1000,tagCol6="empty",tagCol7="empty1"'
        )
        tdSql.execute(f"alter table {tb} set tag tagCol1=0")

        tdSql.query(f"select * from {mt} where tagCol1 = true")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol2 = -1")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol3 = -10")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol4 = -100")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {mt} where tagCol5 = -1000")
        tdSql.checkRows(0)

        tdSql.query(f'select * from {mt} where tagCol6 = "empty"')
        tdSql.checkRows(0)

        tdSql.query(f'select * from {mt} where tagCol6 = "empty1"')
        tdSql.checkRows(0)

        tdSql.execute(f"insert into {tbj} values (now, 1)")

        tdSql.query(f"select * from {mt} where tagCol1 = true")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol2 = -1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol3 = -10")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol4 = -100")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol5 = -1000")
        tdSql.checkRows(1)

        tdSql.query(f'select * from {mt} where tagCol6 = "empty"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from {mt} where tagCol7 = "empty1"')
        tdSql.checkRows(1)

        tdSql.execute(f"alter table {tbj} set tag tagCol1=true")
        tdSql.execute(f"alter table {tb} set tag tagCol1=true")

        tdSql.query(f"select * from {mt} where tagCol1 = true")
        tdSql.checkRows(3)

        tdSql.execute(f"alter table {tb} set tag tagCol1=false")

        tdSql.execute(
            f'alter table {tbj} set tag tagCol1=true,tagCol2=-10,tagcol3=-100, tagcol4=-1000,tagcol5=-10000,tagCol6="empty1",tagCol7="empty2"'
        )

        tdSql.query(f"select * from {mt} where tagCol1 = true")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol2 = -10")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol3 = -100")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol4 = -1000")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol5 = -10000")
        tdSql.checkRows(1)

        tdSql.query(f'select * from {mt} where tagCol6 = "empty1"')
        tdSql.checkRows(1)

        tdSql.query(f'select * from {mt} where tagCol7 = "empty2"')
        tdSql.checkRows(1)

        tdSql.execute(
            f"alter table {tbj} set tag tagCol1=true,tagCol2=-10,tagcol3=-100, tagcol4=-1000,tagcol5=NULL,tagCol6=NULL,tagCol7=NULL"
        )
        tdSql.execute(
            f"alter table {tbj} set tag `tagcol1`=true,`tagcol2`=-10,`tagcol3`=-100, `tagcol4`=-1000,`tagcol5`=NULL,`tagcol6`=NULL,`tagcol7`=NULL"
        )

        tdSql.execute(f"alter table {mt} drop tag tagCol7")
        tdSql.execute(f"alter table {mt} drop tag tagCol3")

        tdSql.execute(f"alter table {mt} add tag tagCol8 int")

        # set not exist tag and value
        tdSql.error(
            f"alter table {tbj} set tag tagCol1=true,tagCol2=-10,tagcol3=-100, tagcol4=-1000,tagcol5=NULL,tagCol6=NULL,tagCol7=NULL"
        )
        tdSql.error(
            f"alter table {tbj} set tag tagCol1=true,tagCol2=-10,tagcol3=-100, tagcol4=-1000,tagcol5=NULL,tagCol6=NULL,tagCol7=NULL"
        )

        tdSql.execute(f"alter table {tbj} set tag tagCol8 = 8")

        tdSql.query(f"select * from {mt} where tagCol4 = -1000")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tagCol8 = 8")
        tdSql.checkRows(1)
