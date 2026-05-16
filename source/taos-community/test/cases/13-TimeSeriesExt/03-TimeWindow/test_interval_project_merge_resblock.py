from new_test_framework.utils import tdLog, tdSql


class TestIntervalProjectMergeResblock:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _assert_projection_merge_resblock_false(self, plan_rows):
        projection_found = False
        merge_flag_found = False

        for i, row in enumerate(plan_rows):
            plan = str(row)
            if "-> Projection" not in plan:
                continue

            projection_found = True
            for j in range(i + 1, len(plan_rows)):
                plan_line = str(plan_rows[j])
                if "-> " in plan_line:
                    # Stop at the next operator plan line.
                    break
                if "Merge ResBlocks:" not in plan_line:
                    continue
                merge_flag_found = True
                if "False" not in plan_line:
                    tdLog.exit(
                        f"Projection Merge ResBlocks should be False, but got: {plan_line}"
                    )
                break

        if not projection_found:
            tdLog.exit(f"Projection node not found in explain plan: {plan_rows}")

        if not merge_flag_found:
            tdLog.exit(f"Projection Merge ResBlocks line not found in explain plan: {plan_rows}")

        tdLog.info("Projection Merge ResBlocks is correctly set to False in the explain plan.")

    def test_interval_project_merge_resblock(self):
        """Interval: project merge resblock flag

        1. Build a subquery + interval shape that has a non-terminal projection node
        2. Verify explain plan sets Projection Merge ResBlocks to False

        Since: v3.0.0.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6825791030

        History:
            - 2026-02-27 Added for regression of interval hang caused by project merge

        """
        db = "db_interval_proj_merge"
        stb = "stable_1"

        tdSql.prepare(db, drop=True, vgroups=2)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create stable {stb} (ts timestamp, c1 int) tags (tg int)")
        tdSql.execute(f"create table {stb}_1 using {stb} tags(1)")
        tdSql.execute(f"create table {stb}_2 using {stb} tags(2)")

        tdSql.execute(
            f"insert into {stb}_1 values "
            "('2021-08-27 01:46:40.000', 1) "
            "('2021-09-01 01:46:40.000', 2) "
            "('2021-12-24 12:06:20.000', 3)"
        )
        tdSql.execute(
            f"insert into {stb}_2 values "
            "('2021-08-27 01:46:39.999', 1) "
            "('2021-09-01 01:46:39.999', 2) "
            "('2021-12-24 12:06:20.001', 3)"
        )

        sql = (
            f"explain verbose true "
            f"select _wstart,_wend,count(c1) from (select * from {stb}) interval(44s) sliding(1s)"
        )
        tdSql.query(sql, queryTimes=1)
        self._assert_projection_merge_resblock_false(tdSql.queryResult)
