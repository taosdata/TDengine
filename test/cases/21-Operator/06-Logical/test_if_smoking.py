from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestIfSmoking:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_if_smoking(self):
        """If

        1. Using in data columns and scalar functions within SELECT statements
        2. Using in data columns within WHERE conditions
        3. Using in data columns within GROUP BY statements
        4. Using in data columns within STATE WINDOW
        5. Using in aggregate functions while including the IS NULL operator

        Catalog:
            - Operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-10 Stephen Jin

        """

        self.If()
        self.IfNull()
        self.Nvl()
        self.Nvl2()
        self.NullIf()
        self.IsNull()
        self.IsNotNull()
        self.Coalesce()

    def If(self):
        tdSql.query(f"SELECT IF(1>2,2,3)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"SELECT IF(1<2,'yes','no')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'yes')

        tdSql.query(f"SELECT IF(1>2,'yes','no')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'no')

    def IfNull(self):
        tdSql.query(f"select ifnull(1, 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select ifnull(null, 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select ifnull(1/0, 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select ifnull(1/0, 'yes');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'yes')

    def Nvl(self):
        tdSql.query(f"select nvl(1, 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select nvl(null, 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select nvl(1/0, 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select nvl(1/0, 'yes');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'yes')

    def Nvl2(self):
        tdSql.query(f"select nvl2(null, 1, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select nvl2('x', 1, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def NullIf(self):
        tdSql.query(f"select nullif(1, 1);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select nullif(1, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def IsNull(self):
        tdSql.query(f"SELECT 1 IS NULL, 0 IS NULL, NULL IS NULL;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 1)

    def IsNotNull(self):
        tdSql.query(f"SELECT 1 IS NOT NULL, 0 IS NOT NULL, NULL IS NOT NULL;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 0)

    def Coalesce(self):
        tdSql.query(f"select coalesce(null, 1);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select coalesce(null, null, null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
