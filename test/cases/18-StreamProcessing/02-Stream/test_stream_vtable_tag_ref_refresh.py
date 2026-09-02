import time

from new_test_framework.utils import tdCom, tdLog, tdSql, tdStream


class TestStreamVtableTagRefRefresh:
    precision = "ms"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _wait_for_value(self, db, expected, timeout=45):
        deadline = time.time() + timeout
        last_values = []
        while time.time() < deadline:
            tdSql.query(f"select value from {db}.out_b order by ts", queryTimes=1)
            last_values = [tdSql.getData(index, 0) for index in range(tdSql.getRows())]
            if expected in last_values:
                return
            time.sleep(1)
        tdLog.exit(
            f"timed out waiting for stream value {expected}, observed values: {last_values}"
        )

    def do_tag_ref_source_tag_change_refreshes_stream_calculation(self):
        db = "stream_vtb_tagref_refresh"
        tdCom.create_snode_if_not_exists()
        tdSql.execute(f"drop database if exists {db}")

        try:
            tdSql.execute(f"create database {db} precision '{self.precision}' vgroups 1")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable src_st (ts timestamp, vol double, st tinyint) "
                "tags (element_name varchar(64))"
            )
            tdSql.execute(
                "create stable cfg_st (ts timestamp, dummy int) "
                "tags (oil_thresh double, water_thresh double)"
            )
            tdSql.execute("create table src_oil using src_st tags ('Well 01 - Oil')")
            tdSql.execute("create table c_cfg using cfg_st tags (25.0, 30.0)")
            tdSql.execute("insert into c_cfg values (now, 0)")
            tdSql.execute(
                "create stable vst (ts timestamp, vol double, st tinyint) "
                "tags (element_name varchar(64), oil_thresh double, water_thresh double) virtual 1"
            )
            tdSql.execute(
                "create vtable vtb_oil (vol from src_oil.vol, st from src_oil.st) "
                "using vst (element_name, oil_thresh, water_thresh) "
                "tags ('Well 01 - Oil', c_cfg.oil_thresh, c_cfg.water_thresh)"
            )

            tdSql.execute(
                "create stream s_tagref_refresh count_window(1) from vst partition by tbname "
                "into out_b as "
                "select cast(_tlocaltime/1000000 as timestamp) as ts, "
                "case when last(st) != 0 then null "
                "when last(element_name) like '%Oil%' then floor(last(vol)/last(oil_thresh)) "
                "else floor(last(vol)/last(water_thresh)) end as value, "
                "cast(case when last(st) != 0 then -1 else 0 end as tinyint) as status, "
                "last(element_name) as element_name from %%tbname where st >= -1"
            )

            tdLog.info("wait for stream to become ready before inserting source data")
            tdStream.checkStreamStatus("s_tagref_refresh")

            tdLog.info("verify the initial count-window calculation uses the referenced threshold")
            tdSql.execute("insert into src_oil values (now, 100.5, 0)")
            self._wait_for_value(db, 4.0)

            tdLog.info("change the tag-ref source and verify direct virtual-table visibility")
            tdSql.execute("alter table c_cfg set tag oil_thresh = 40.0")
            tdSql.query("select distinct oil_thresh from vst")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 40.0)

            tdLog.info("verify the next %%tbname calculation uses the refreshed referenced threshold")
            tdSql.execute("insert into src_oil values (now, 100.5, 0)")
            self._wait_for_value(db, 2.0)
        finally:
            tdSql.execute(f"drop database if exists {db} force")

        print("tag-ref count-window refresh .............. [ passed ]")

    def test_tag_ref_source_tag_change_refreshes_stream_calculation(self):
        """Tag-ref source tag changes refresh a running %%tbname count-window calculation.

        1. Creates a virtual child whose threshold tags reference a config child table
        2. Confirms direct virtual-table queries observe the updated config tag
        3. Requires the next count-window calculation to use the updated threshold

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.2.5

        Labels: common,ci,stream,vtable

        Jira: None

        History:
            - 2026-08-26 Wang Mingming Added tag-ref refresh regression coverage

        """
        self.do_tag_ref_source_tag_change_refreshes_stream_calculation()
