import time

from new_test_framework.utils import tdLog, tdSql, tdStream, clusterComCheck


class TestSubqueryStateEmptyResult:
    DB = "bug_extwin_empty"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_subquery_state_empty_result(self):
        """Subquery: state_window empty result should not be emitted

        1. Build a stream query with external-window style subqueries.
        2. Trigger one state window with data that does not satisfy `aa >= 95`.
        3. Verify stream output remains empty.

        Catalog:
            - Streams:SubQuery

        Since: v3.3.6.x

        Labels: common,ci

        Jira: https://project.feishu.cn/taosdata_td/defect/detail/6799007996

        """

        self.prepare_env()
        self.create_tables()
        self.create_stream()
        self.insert_data()
        self.check_no_output()

    def prepare_env(self):
        tdStream.createSnode(1)
        tdSql.prepare(dbname=self.DB, vgroups=1)
        clusterComCheck.checkDbReady(self.DB)
        tdSql.execute(f"use {self.DB}")

    def create_tables(self):
        tdSql.execute(
            """
            CREATE STABLE s_0x13 (
                ts TIMESTAMP,
                trade_no VARCHAR(20),
                soc INT,
                consume DOUBLE
            ) TAGS (
                gun_code VARCHAR(20),
                threshold INT
            );
            """
        )

        tdSql.execute(
            """
            CREATE STABLE s_0x15 (
                ts TIMESTAMP,
                battery_voltage DOUBLE,
                battery_capacity DOUBLE
            ) TAGS (
                gun_code VARCHAR(20),
                battery_capacity_rate_constant FLOAT,
                battery_capacity_correction_nominal FLOAT
            );
            """
        )

    def create_stream(self):
        tdSql.execute(
            """
            CREATE STREAM st_degra STATE_WINDOW(trade_no)
            FROM s_0x13
                PARTITION BY tbname, gun_code
                STREAM_OPTIONS(MAX_DELAY(15s))
            INTO st_degra
                OUTPUT_SUBTABLE(CONCAT('st_degra_', %%1))
            AS
            SELECT
                ts,
                trade_no,
                degra_rate
            FROM (
                SELECT
                    a.ts,
                    b.trade_no,
                    CASE
                        WHEN b.c_soc IS NOT NULL
                        THEN b.consume/(b.c_soc-a.battery_capacity_correction_nominal)/a.battery_capacity*100
                    END degra_rate
                FROM (
                    SELECT
                        _twstart ts,
                        LAST(battery_capacity_correction_nominal) battery_capacity_correction_nominal,
                        LAST(battery_capacity)*LAST(battery_voltage)*(1+LAST(battery_capacity_rate_constant)) battery_capacity
                    FROM s_0x15
                    WHERE gun_code=%%2 AND _c0>=_twstart AND _c0<_twend
                ) a
                JOIN (
                    SELECT
                        ts,
                        (aa-bb) c_soc,
                        trade_no,
                        consume
                    FROM (
                        SELECT
                            _twstart ts,
                            LAST(trade_no) trade_no,
                            LAST(consume) consume,
                            LAST(soc) aa,
                            FIRST(soc) bb
                        FROM %%tbname
                        WHERE _c0>=_twstart AND _c0<_twend AND soc<=95 AND soc>0
                    )
                    WHERE aa>=95
                ) b
                ON a.ts=b.ts
            )
            WHERE degra_rate IS NOT NULL;
            """
        )

        tdStream.checkStreamStatus()

    def insert_data(self):
        tdSql.execute(
            """
            INSERT INTO s_0x15_abc USING s_0x15 TAGS ('abc', 0.1, 0.2)
            (ts, battery_capacity, battery_voltage)
            VALUES ('2026-01-01 00:05:00', 100.0, 400.0);
            """
        )

        tdSql.execute(
            """
            INSERT INTO s_0x13_abc USING s_0x13 TAGS ('abc', 10)
            (ts, trade_no, consume, soc) VALUES
                ('2026-01-01 00:00:00', 'trade_001', 20.0, 30),
                ('2026-01-01 00:10:00', 'trade_001', 50.0, 80),
                ('2026-01-01 00:20:00', 'trade_001', 80.0, 90),
                ('2026-01-01 00:30:00', 'trade_002', 30.0, 40);
            """
        )

    def check_no_output(self):
        # verify no late output appears
        no_row_sql = f"select * from {self.DB}.st_degra;"

        for _ in range(30):
            tdSql.query(no_row_sql)
            if tdSql.getRows() != 0:
                raise Exception(
                    f"unexpected {tdSql.getRows()} stream output rows found in st_degra"
                )
            time.sleep(1)
