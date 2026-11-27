from new_test_framework.utils import tdLog, tdSql

from collections import Counter
from datetime import datetime
import json


class TestJoinJsonTagConditions:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def prepare(self):
        tdSql.execute("drop database if exists join_json_tag_conditions")
        tdSql.execute("create database join_json_tag_conditions vgroups 2")
        tdSql.execute("use join_json_tag_conditions")
        tdSql.execute("create table st1(ts timestamp, f int) tags (t json)")
        tdSql.execute("create table st2(ts timestamp, f int) tags (t json)")
        tdSql.execute(
            "insert into ct11 using st1 tags('{\"tag1\":\"1-11\",\"tag2\":1}') "
            "values('2023-10-16 09:10:11', 11)('2023-10-16 09:10:12', 12)"
        )
        tdSql.execute(
            "insert into ct12 using st1 tags('{\"tag1\":\"1-12\",\"tag2\":2}') "
            "values('2023-10-16 09:10:11', 21)('2023-10-16 09:10:12', 22)"
        )
        tdSql.execute(
            "insert into ct21 using st2 tags('{\"tag1\":\"1-21\",\"tag2\":1}') "
            "values('2023-10-16 09:10:11', 110)('2023-10-16 09:10:12', 120)"
        )
        tdSql.execute(
            "insert into ct22 using st2 tags('{\"tag1\":\"1-22\",\"tag2\":2}') "
            "values('2023-10-16 09:10:11', 210)('2023-10-16 09:10:12', 220)"
        )
        tdSql.execute(
            "insert into ct23 using st2 tags('{\"tag1\":\"1-11\",\"tag2\":3}') "
            "values('2023-10-16 09:10:11', 310)('2023-10-16 09:10:12', 320)"
        )

    @staticmethod
    def check_right_timestamps(expected, numeric_tag=False, window_join=False):
        tdSql.checkRows(len(expected))

        def timestamp(value):
            if value is None or isinstance(value, datetime):
                return value
            return datetime.fromisoformat(value)

        def json_value(value):
            if value is None:
                return None
            try:
                return json.loads(value)
            except (TypeError, ValueError):
                return value

        actual = []
        for row in range(tdSql.queryRows):
            left_ts, left_tag, right_ts, right_tag = (tdSql.getData(row, col) for col in range(4))
            actual.append((timestamp(left_ts), json_value(left_tag), timestamp(right_ts),
                           json_value(right_tag)))

        # Each matching right table has rows at both prepared timestamps.
        times = [datetime(2023, 10, 16, 9, 10, second) for second in (11, 12)]
        tags = {"1-11": "1-21", "1-12": "1-22"} if numeric_tag else {"1-11": "1-11", "1-12": None}
        expected_rows = []
        for left_tag, right_tag in tags.items():
            for left_ts in times:
                if right_tag is None:
                    expected_rows.append((left_ts, left_tag, None, None))
                else:
                    matches = [ts for ts in times if 0 <= (ts - left_ts).total_seconds() <= 1] if window_join else [left_ts]
                    expected_rows.extend((left_ts, left_tag, right_ts, right_tag) for right_ts in matches)
        if Counter(row[2] for row in expected_rows) != Counter(timestamp(ts) for ts in expected):
            raise AssertionError("JSON tag fixture and expected timestamps disagree")
        if Counter(actual) != Counter(expected_rows):
            raise AssertionError(f"JSON tag join mismatch: expected={expected_rows}, actual={actual}")

    # --- impl ---

    def do_asof_string_tag(self):
        tdSql.query(
            "select a.ts, a.t->'tag1', b.ts, b.t->'tag1' "
            "from st1 a left asof join st2 b on a.t->'tag1' = b.t->'tag1'"
        )
        self.check_right_timestamps(
            ["2023-10-16 09:10:11.000", "2023-10-16 09:10:12.000", None, None]
        )

    def do_asof_numeric_tag(self):
        tdSql.query(
            "select a.ts, a.t->'tag1', b.ts, b.t->'tag1' "
            "from st1 a left asof join st2 b on a.t->'tag2' = b.t->'tag2'"
        )
        self.check_right_timestamps(
            [
                "2023-10-16 09:10:11.000",
                "2023-10-16 09:10:12.000",
                "2023-10-16 09:10:11.000",
                "2023-10-16 09:10:12.000",
            ],
            numeric_tag=True,
        )

    def do_window_string_tag(self):
        tdSql.query(
            "select a.ts, a.t->'tag1', b.ts, b.t->'tag1' "
            "from st1 a left window join st2 b on a.t->'tag1' = b.t->'tag1' "
            "window_offset(0s, 1s)"
        )
        self.check_right_timestamps(
            [
                "2023-10-16 09:10:11.000",
                "2023-10-16 09:10:12.000",
                "2023-10-16 09:10:12.000",
                None,
                None,
            ],
            window_join=True,
        )

    def do_window_numeric_tag(self):
        tdSql.query(
            "select a.ts, a.t->'tag1', b.ts, b.t->'tag1' "
            "from st1 a left window join st2 b on a.t->'tag2' = b.t->'tag2' "
            "window_offset(0s, 1s)"
        )
        self.check_right_timestamps(
            [
                "2023-10-16 09:10:11.000",
                "2023-10-16 09:10:12.000",
                "2023-10-16 09:10:12.000",
                "2023-10-16 09:10:11.000",
                "2023-10-16 09:10:12.000",
                "2023-10-16 09:10:12.000",
            ],
            numeric_tag=True,
            window_join=True,
        )

    # --- main ---

    def test_asof_string_json_tag_condition(self):
        """ASOF JOIN with a string JSON tag condition

        1. Verify a JSON string sub-property is accepted as an ASOF JOIN condition.

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-14 Added regression coverage for JSON tag join conditions.

        """
        self.prepare()
        self.do_asof_string_tag()

    def test_asof_numeric_json_tag_condition(self):
        """ASOF JOIN with a numeric JSON tag condition

        1. Verify a JSON numeric sub-property is accepted as an ASOF JOIN condition.

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-14 Added regression coverage for JSON tag join conditions.

        """
        self.prepare()
        self.do_asof_numeric_tag()

    def test_window_string_json_tag_condition(self):
        """WINDOW JOIN with a string JSON tag condition

        1. Verify a JSON string sub-property is accepted as a WINDOW JOIN condition.

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-14 Added regression coverage for JSON tag join conditions.

        """
        self.prepare()
        self.do_window_string_tag()

    def test_window_numeric_json_tag_condition(self):
        """WINDOW JOIN with a numeric JSON tag condition

        1. Verify a JSON numeric sub-property is accepted as a WINDOW JOIN condition.

        Catalog:
            - Query:Join

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-14 Added regression coverage for JSON tag join conditions.

        """
        self.prepare()
        self.do_window_numeric_tag()
