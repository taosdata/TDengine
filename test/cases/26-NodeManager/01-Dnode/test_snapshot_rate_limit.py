from new_test_framework.utils import tdLog, tdSql


class TestSnapshotRateLimit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_alter_snapshot_rate_limit(self):
        """Test snapshotRateLimit parameter via ALTER DNODE

        Verify that the snapshotRateLimit parameter can be set, queried,
        and validated through ALTER DNODE / ALTER ALL DNODES commands.

        Catalog:
            - ManageNodes:Dnode

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Created

        """

        tdLog.info("======== step1: verify default value is 0")
        tdSql.query("show dnode 1 variables")
        found = False
        for row in tdSql.queryResult:
            if row[1] == "snapshotRateLimit":
                tdLog.info(f"snapshotRateLimit current value: {row[2]}")
                assert str(row[2]) == "0", f"expected default 0, got {row[2]}"
                found = True
                break
        assert found, "snapshotRateLimit not found in dnode variables"

        tdLog.info("======== step2: alter single dnode should fail (global param)")
        # snapshotRateLimit is CFG_CATEGORY_GLOBAL, cannot alter on single dnode
        tdSql.error("alter dnode 1 'snapshotRateLimit' '50'")

        tdLog.info("======== step3: alter all dnodes snapshotRateLimit to 50")
        tdSql.execute("alter all dnodes 'snapshotRateLimit' '50'")
        tdSql.query("show dnode 1 variables")
        for row in tdSql.queryResult:
            if row[1] == "snapshotRateLimit":
                assert str(row[2]) == "50", f"expected 50, got {row[2]}"
                break

        tdLog.info("======== step4: alter all dnodes to 100")
        tdSql.execute("alter all dnodes 'snapshotRateLimit' '100'")
        tdSql.query("show dnode 1 variables")
        for row in tdSql.queryResult:
            if row[1] == "snapshotRateLimit":
                assert str(row[2]) == "100", f"expected 100, got {row[2]}"
                break

        tdLog.info("======== step5: alter to max value 10240")
        tdSql.execute("alter all dnodes 'snapshotRateLimit' '10240'")
        tdSql.query("show dnode 1 variables")
        for row in tdSql.queryResult:
            if row[1] == "snapshotRateLimit":
                assert str(row[2]) == "10240", f"expected 10240, got {row[2]}"
                break

        tdLog.info("======== step6: alter to 0 (disable rate limit)")
        tdSql.execute("alter all dnodes 'snapshotRateLimit' '0'")
        tdSql.query("show dnode 1 variables")
        for row in tdSql.queryResult:
            if row[1] == "snapshotRateLimit":
                assert str(row[2]) == "0", f"expected 0, got {row[2]}"
                break

        tdLog.info("======== step7: test invalid values")
        # Value exceeding max range
        tdSql.error("alter all dnodes 'snapshotRateLimit' '10241'")
        # Negative value
        tdSql.error("alter all dnodes 'snapshotRateLimit' '-1'")
        # Non-numeric value
        tdSql.error("alter all dnodes 'snapshotRateLimit' 'abc'")

        tdLog.info("======== step8: verify value unchanged after invalid attempts")
        tdSql.query("show dnode 1 variables")
        for row in tdSql.queryResult:
            if row[1] == "snapshotRateLimit":
                assert str(row[2]) == "0", f"expected 0, got {row[2]}"
                break

        tdLog.info("test_alter_snapshot_rate_limit passed")
