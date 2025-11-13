from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestSelectWithJsonTags:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_with_json_tags(self):
        """Json tag

        1. Create db
        2. Create supper table
        3. Inset data with json tags
        3. Select data and check the result

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6593

        History:
            - 2025-08-04 Ethan liu adds test for test select with json tags

        """

        tdLog.info(f"========== start select with json tags test")
        tdSql.execute(f"drop database if exists test_json_tag")
        tdSql.execute(f"create database test_json_tag")
        tdSql.execute(f"use test_json_tag")

        # create super table and sub table
        tdSql.execute(f"create stable stb(ts timestamp, v int) tags (info json)")
        tdSql.execute('insert into t0 using stb tags(\'{"info":"大连\n123"}\') values (now ,1)')
        tdSql.execute('insert into t1 using stb tags(\'{"info":"大连\t123"}\') values (now ,1)')
        tdSql.execute('insert into t2 using stb tags(\'{"info":"大连\b123"}\') values (now ,1)')
        tdSql.execute('insert into t3 using stb tags(\'{"info":"大连\r123"}\') values (now ,1)')
        tdSql.execute('insert into t4 using stb tags(\'{"info":"大连\f123"}\') values (now ,1)')
        
        # select data and check the result
        tdSql.query(f"select info->'info' from t0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0,'\"大连\\n123\"')
        tdSql.query(f"select info->'info' from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "\"大连\\t123\"")
        tdSql.query(f"select info->'info' from t2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "\"大连\\b123\"")
        tdSql.query(f"select info->'info' from t3")
        tdSql.checkData(0, 0, "\"大连\\r123\"")
        tdSql.query(f"select info->'info' from t4")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "\"大连\\f123\"")

        tdLog.info(f"end select with json tags test successfully")

    def test_select_with_json_tags(self):
        """Operator json

        1. Create db
        2. Create supper table with json data-type tag
        3. Create child table with json tag values
        4. Query with json operators
        5. Check the result value correctly

        Catalog:
            - Operators:Json

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-08-04 Ethan liu adds test for test select with json tags

        """
        pass