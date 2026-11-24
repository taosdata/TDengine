# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest

from new_test_framework.utils import tdSql, tdStream


class TestTimeAlignManual:
    base = 1767225600000  # 2026-01-01 00:00:00 UTC at millisecond precision

    @classmethod
    def setup_class(cls):
        tdStream.ensureSnode(1)

    def setup_method(self):
        self.streams = []

    def teardown_method(self):
        for name in reversed(self.streams):
            tdSql.execute(f"drop stream if exists {name}")

    def _sources(self, db, vgroups=1):
        tdSql.prepare(dbname=db, drop=True, vgroups=vgroups, precision="ms", keep=3650)
        tdSql.execute("create table volts (ts timestamp, value double)")
        tdSql.execute("create table amps (ts timestamp, value double)")
        tdSql.execute(
            "create vtable meter (ts timestamp, voltage double from volts.value, "
            "current_a double from amps.value)"
        )

    def _power_stream(self, name, rules, options="", calc_filter=""):
        tdSql.execute(
            f"create stream {name} interval(10s) sliding(10s) "
            f"from table(time_align(table meter, true, '{rules}')) partition by tbname "
            f"{options} into {name}_out as "
            "select _twstart as ws, count(voltage_p * current_a_p) as samples, "
            "avg(voltage_p * current_a_p) as power_avg "
            f"from table(time_align(table %%tbname, true, '{rules}')) "
            f"where ts >= _twstart and ts < _twend{calc_filter}"
        )
        self.streams.append(name)
        tdStream.checkStreamStatus(name)

    def _power(self, name, offset, samples, mean):
        ts = self.base + offset
        tdSql.checkResultsByFunc(
            sql=f"select cast(ws as bigint) as start_ms, samples, power_avg from {name}_out where ws = {ts}",
            func=lambda: list(tdSql.queryResult) == [(ts, samples, mean)],
            retry=60,
        )
        assert [column[0].lower() for column in tdSql.cursor.description] == ["start_ms", "samples", "power_avg"]

    def test_manual_power_example(self):
        """Read the two aligned power samples in one ten-second window.

        Voltage is 220 at second zero. The current reaches 2 and 3 amperes at
        seconds 1 and 2. A later current reading closes the first window.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._sources("manual_align_power")
        self._power_stream("power_10s", "voltage:PREV:_p;current_a:PREV:_p")
        tdSql.execute(f"insert into volts values ({self.base}, 220)")
        tdSql.execute(f"insert into amps values ({self.base + 1000}, 2) ({self.base + 2000}, 3)")
        tdSql.execute(f"insert into amps values ({self.base + 11000}, 4)")
        self._power("power_10s", 0, 2, 550.0)

    def test_manual_duration_and_filter(self):
        """Keep a two-second voltage candidate through the exact duration boundary.

        A voltage-only row fails the trigger filter. The reading still supplies
        220 volts at two seconds. One millisecond later it has expired. Use a
        fresh voltage and current in the next window to close the first one.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._sources("manual_align_duration")
        tdSql.execute(f"insert into amps values ({self.base + 11000}, 4)")
        self._power_stream(
            "bounded_power",
            "voltage:PREV(2s):_p;current_a:PREV:_p",
            "stream_options(pre_filter(voltage_p = 220 and current_a is not null))",
            " and current_a is not null",
        )
        start = self.base + 60000
        tdSql.execute(f"insert into volts values ({start}, 220)")
        tdSql.execute(f"insert into amps values ({start + 2000}, 2) ({start + 2001}, 3)")
        tdSql.execute(f"insert into volts values ({start + 11000}, 220)")
        tdSql.execute(f"insert into amps values ({start + 12000}, 4)")
        self._power("bounded_power", 60000, 1, 440.0)

    @pytest.mark.parametrize("vgroups", [1, 2])
    def test_manual_rebind(self, vgroups):
        """Run the next meter window with the new voltage reference.

        The replacement already contains 100 volts. A later current opens the
        checked window. The same task must produce it with one vgroup or with
        the current and voltage on different vgroups.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        db = "manual_align_rebind" if vgroups == 1 else "manual_align_rebind_vgroups"
        self._sources(db, vgroups)
        tdSql.execute("create table replacement (ts timestamp, value double)")
        if vgroups == 2:
            tdSql.query(
                "select table_name, vgroup_id from information_schema.ins_tables "
                f"where db_name = '{db}' and table_name in ('volts', 'amps', 'replacement')"
            )
            locations = dict(tdSql.queryResult)
            assert locations["volts"] == locations["replacement"] != locations["amps"]
        start = self.base + 100000
        tdSql.execute(f"insert into replacement values ({start}, 100)")
        self._power_stream(
            "rebound_power",
            "voltage:PREV:_p;current_a:PREV:_p",
            "stream_options(pre_filter(voltage_p = 100 and current_a is not null))",
            " and voltage_p = 100 and current_a is not null",
        )
        running_task = (
            "select task_id from information_schema.ins_stream_tasks "
            "where stream_name = 'rebound_power' and type = 'Trigger' and status = 'Running'"
        )
        tdSql.query(running_task)
        assert len(tdSql.queryResult) == 1
        initial_task = tdSql.queryResult[0][0]
        tdSql.execute(f"insert into volts values ({start + 1000}, 220)")
        tdSql.execute(f"insert into amps values ({start + 2000}, 2)")
        tdSql.execute("alter vtable meter alter column voltage set replacement.value")
        tdSql.execute(f"insert into amps values ({start + 21000}, 3)")
        tdSql.execute(f"insert into replacement values ({start + 22000}, 100)")
        tdSql.execute(f"insert into amps values ({start + 31000}, 4)")
        self._power("rebound_power", 120000, 1, 300.0)
        tdSql.query(running_task)
        assert list(tdSql.queryResult) == [(initial_task,)]

    def test_manual_restrictions(self):
        """Reject a trigger row snapshot and a filtered periodic aligned trigger.

        Use the same working local virtual table and alignment rules as the
        power example. Each statement requests one unsupported combination.

        Catalog:
            - Streams:Trigger:TimeAlign

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7163362578

        History:
            - 2026-11-19 OpenAI Created
        """
        self._sources("manual_align_restricted")
        source = "table(time_align(table meter, true, 'voltage:PREV:_p;current_a:PREV:_p'))"
        tdSql.error(
            f"create stream power_rows interval(10s) sliding(10s) from {source} "
            "into power_rows_out as select _twstart as ws, count(*) as n from %%trows",
            expectedErrno=0x410F,
        )
        tdSql.error(
            f"create stream power_period period(10s) from {source} "
            "stream_options(pre_filter(voltage_p > 0)) into power_period_out as "
            "select cast(_tlocaltime / 1000000 as timestamp) as ws, count(*) as n from meter",
            expectedErrno=0x410E,
        )
