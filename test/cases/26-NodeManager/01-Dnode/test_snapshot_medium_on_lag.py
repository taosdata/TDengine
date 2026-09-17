import time

from new_test_framework.utils import tdLog, tdSql

PARAM = "snapshotMediumOnLag"


class TestSnapshotMediumOnLag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------ helpers

    def _dnode_ids(self):
        """Return the id of every dnode of the cluster, in the order reported by the mnode."""
        tdSql.query("show dnodes")
        return [int(row[0]) for row in tdSql.queryResult]

    def _param_value(self, dnode_id):
        """Return the value of snapshotMediumOnLag as reported by the given dnode."""
        tdSql.query(f"show dnode {dnode_id} variables like '{PARAM}'")
        tdSql.checkRows(1)
        return str(tdSql.queryResult[0][2])

    def _set_param(self, value):
        """Set snapshotMediumOnLag for all dnodes and wait until it becomes visible."""
        tdSql.execute(f"alter all dnodes '{PARAM}' '{value}'")
        # The ALTER is applied to every dnode asynchronously, so poll until it is visible.
        for attempt in range(30):
            actual = self._param_value(1)
            if actual == value:
                tdLog.info(f"{PARAM} is now {actual} (attempt {attempt + 1})")
                return
            time.sleep(1)
        raise AssertionError(
            f"expected {PARAM}={value}, but dnode 1 still reports {actual} after 30s"
        )

    def _check_param_on_all_dnodes(self, expected):
        """Assert that every dnode reports the expected value for the parameter."""
        dnode_ids = self._dnode_ids()
        assert dnode_ids, "no dnode was reported by the mnode"
        for dnode_id in dnode_ids:
            actual = self._param_value(dnode_id)
            assert actual == expected, (
                f"Expected {PARAM}={expected} on dnode {dnode_id}, but it reports {actual}"
            )
        tdLog.info(f"{PARAM}={expected} on every dnode: {dnode_ids}")

    # --------------------------------------------------------------------- tests

    def test_snapshot_medium_on_lag_param(self):
        """snapshotMediumOnLag parameter visibility and dynamic update

        1. Verify the default value is 0 (feature disabled)
        2. Verify ALTER on a single dnode is rejected (the parameter is global)
        3. Enable it for all dnodes and verify every dnode reports the new value
        4. Disable it again and verify every dnode reports the restored value

        Catalog:
            - ManageNodes:Dnode

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-10 cadem Added snapshotMediumOnLag parameter coverage.

        """
        tdLog.info("======== step1: default value must be 0 (feature disabled)")
        self._check_param_on_all_dnodes("0")

        tdLog.info("======== step2: altering a single dnode must fail for a global parameter")
        tdSql.error(f"alter dnode 1 '{PARAM}' '1'")
        self._check_param_on_all_dnodes("0")

        tdLog.info("======== step3: enable it for all dnodes")
        self._set_param("1")
        self._check_param_on_all_dnodes("1")

        tdLog.info("======== step4: disable it again")
        self._set_param("0")
        self._check_param_on_all_dnodes("0")

        print("snapshotMediumOnLag parameter ................... [ passed ]")
