from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestBinaryEscapeCharacter:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_binary_escape_character(self):
        """Escape character

        1. Validates escape characters in binary data types
        2. Test the insertion and retrieval of strings containing various escape sequences like single quotes ('), double quotes ("), and backslashes () within binary columns.
        3. Ensures that these special characters are correctly stored, processed, and returned in query results without causing parsing errors or data corruption.

        Catalog:
            - EscapeCharacters

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/binary_escapeCharacter.sim

        """

        tdSql.execute(f"drop database if exists ecdb")
        tdSql.execute(f"create database ecdb")
        tdSql.execute(f"use ecdb")

        s1 = "\\'"
        s2 = "\\'abc"
        s3 = "123\\'"
        tdSql.execute(f"create table tbx (ts timestamp, c1 int, c2 binary(20))")
        tdSql.execute(f"insert into tbx values ('2019-10-05 18:00:01.000', 1, '{s1}')")
        tdSql.execute(f"insert into tbx values ('2019-10-05 18:00:02.000', 2, '{s2}')")
        tdSql.execute(f"insert into tbx values ('2019-10-05 18:00:03.000', 3, '{s3}')")
        tdSql.query(f"select * from tbx")
        tdLog.info(
            f'"=====rows:{tdSql.getRows()}), line0:{tdSql.getData(0,2)}, line1:{tdSql.getData(1,2)}, line2:{tdSql.getData(2,2)}"'
        )
        tdSql.checkData(0, 2, "'")
        tdSql.checkData(1, 2, "'abc")
        tdSql.checkData(2, 2, "123'")

        tdSql.execute(f"create table tb (ts timestamp, c1 binary(20))")
        s1 = "abc''001"
        s2 = "abc\\'002"
        s3 = "abc\\\\003"
        s4 = 'abc"004'
        s5 = "abc\\005"
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:01.000', '{s1}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:02.000', '{s2}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:03.000', '{s3}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:04.000', '{s4}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:05.000', '{s5}')")

        s1 = 'udp\\"001'
        s2 = 'udp\\"002'
        s3 = "udp\\\\003"
        s4 = "udp\\'004"
        s5 = "udp\\005"
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:06.000', '{s1}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:07.000', '{s2}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:08.000', '{s3}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:09.000', '{s4}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:10.000', '{s5}')")

        tdSql.query(f"select * from tb limit 5")
        tdLog.info(f'"====rows: {tdSql.getRows()}) "')
        tdSql.checkRows(5)

        tdLog.info(
            f'"Single quotation ==== tdSql.getData(0,1)~05: {tdSql.getData(0,1)}, {tdSql.getData(1,1)}, {tdSql.getData(2,1)}, {tdSql.getData(3,1)}, {tdSql.getData(4,1)} "'
        )
        tdSql.checkData(0, 1, "abc'001")
        tdSql.checkData(1, 1, "abc'002")
        tdSql.checkData(2, 1, "abc\\003")
        tdSql.checkData(3, 1, 'abc"004')
        tdSql.checkData(4, 1, "abc005")

        tdSql.query(f"select * from tb limit 5 offset 5")
        tdSql.checkRows(5)
        tdLog.info(
            f'"Double quotation  ==== tdSql.getData(0,1)~05: {tdSql.getData(0,1)}, {tdSql.getData(1,1)}, {tdSql.getData(2,1)}, {tdSql.getData(3,1)}, {tdSql.getData(4,1)} "'
        )
        tdSql.checkData(0, 1, 'udp"001')
        tdSql.checkData(1, 1, 'udp"002')
        tdSql.checkData(2, 1, "udp\\003")
        tdSql.checkData(3, 1, "udp'004")
        tdSql.checkData(4, 1, "udp005")

        tdLog.info(f"---------------------> TD-3967")
        tdSql.execute(f"insert into tb values(now, '\\abc\\\\');")
        tdSql.execute(f"insert into tb values(now, '\\abc\\\\');")
        tdSql.execute(f"insert into tb values(now, '\\\\');")

        tdLog.info(f"------------->sim bug")
        # sql_error insert into tb values(now, '\\\');
        tdSql.error(f"insert into tb values(now, '\\');")
        # sql_error insert into tb values(now, '\\\n');
        tdSql.execute(f"insert into tb values(now, '\n');")
