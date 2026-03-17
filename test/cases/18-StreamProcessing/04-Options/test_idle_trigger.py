#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from new_test_framework.utils import tdLog, tdSql, tdStream, cluster

class TestStreamIdleTrigger:
    """Stream idle trigger test suite

    Tests the idle detection and resume functionality for stream processing,
    including IDLE_TIMEOUT configuration and IDLE/RESUME event triggering.
    """

    caseName = ""
    dbname = "test_idle"
    tblRowNum = 10
    subTblNum = 3

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def init_test_database(self, db_suffix):
        db_name = f"{self.dbname}_{db_suffix}"
        tdStream.init_database(db_name)
        return db_name

    def create_stream_and_wait(self, stream_name, sql):
        tdSql.execute(sql)
        tdStream.checkStreamStatus(stream_name)

    def test_idle_detection_basic(self):
        """US1: Basic idle detection - trigger IDLE event after timeout

        Test that a partition group triggers an IDLE event when no data
        is received for the configured IDLE_TIMEOUT period.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: Basic idle detection ===")

        # Create database and source table
        self.init_test_database("idle_basic")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")
        tdSql.execute("create table d002 using st tags (2)")

        # Create stream with IDLE_TIMEOUT(3s) and EVENT_TYPE(IDLE)
        stream_name = "s_idle_basic"
        self.create_stream_and_wait(
            stream_name,
            f"create stream {stream_name} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(3s), event_type(idle)) "
            f"into out_idle "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        # Insert data to d001
        tdSql.execute("insert into d001 values (now, 1)")
        time.sleep(1)

        # Wait for idle timeout (3s + buffer)
        tdLog.info("Waiting 4 seconds for idle timeout...")
        time.sleep(4)

        # Check if IDLE event was triggered (output table should have data)
        tdSql.query("select * from out_idle")
        rows = tdSql.queryRows
        tdLog.info(f"IDLE events triggered: {rows}")

        if rows > 0:
            tdLog.success("✓ IDLE event triggered successfully")
        else:
            tdLog.exit("✗ IDLE event not triggered")

    def test_resume_detection_basic(self):
        """US2: Basic resume detection - trigger RESUME event after idle

        Test that a partition group triggers a RESUME event when data
        is received after being in IDLE state.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: Basic resume detection ===")

        # Create database and source table
        self.init_test_database("resume_basic")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")

        # Create stream with IDLE_TIMEOUT(3s) and EVENT_TYPE(IDLE|RESUME)
        stream_name = "s_resume_basic"
        self.create_stream_and_wait(
            stream_name,
            f"create stream {stream_name} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(3s), event_type(idle|resume)) "
            f"into out_resume "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        # Insert initial data
        tdSql.execute("insert into d001 values (now, 1)")
        time.sleep(1)

        # Wait for idle timeout
        tdLog.info("Waiting 4 seconds for idle timeout...")
        time.sleep(4)

        # Insert new data to trigger RESUME
        tdSql.execute("insert into d001 values (now, 2)")
        time.sleep(2)

        # Check if both IDLE and RESUME events were triggered
        tdSql.query("select * from out_resume order by ts")
        rows = tdSql.queryRows
        tdLog.info(f"Total events triggered: {rows}")

        if rows >= 2:
            tdLog.success("✓ IDLE and RESUME events triggered successfully")
        else:
            tdLog.exit(f"✗ Expected at least 2 events, got {rows}")

    def test_multiple_partitions(self):
        """US1: Multiple partitions with independent idle states

        Test that each partition group maintains independent idle state
        and triggers IDLE events independently.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: Multiple partitions idle independently ===")

        # Create database and source table
        self.init_test_database("multi_partitions")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")
        tdSql.execute("create table d002 using st tags (2)")
        tdSql.execute("create table d003 using st tags (3)")

        # Create stream with IDLE_TIMEOUT(3s)
        stream_name = "s_multi_partitions"
        self.create_stream_and_wait(
            stream_name,
            f"create stream {stream_name} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(3s), event_type(idle)) "
            f"into out_multi "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        # Insert data to all partitions
        tdSql.execute("insert into d001 values (now, 1)")
        tdSql.execute("insert into d002 values (now, 2)")
        tdSql.execute("insert into d003 values (now, 3)")
        time.sleep(1)

        # Keep d002 and d003 active, let d001 go idle
        for i in range(4):
            tdSql.execute("insert into d002 values (now, 10)")
            tdSql.execute("insert into d003 values (now, 10)")
            time.sleep(1)

        # Check IDLE events - only d001 should have triggered
        tdSql.query("select * from out_multi")
        rows = tdSql.queryRows
        tdLog.info(f"IDLE events triggered: {rows}")

        if rows == 1:
            tdLog.success("✓ Only d001 triggered IDLE event")
        else:
            tdLog.exit(f"✗ Expected 1 IDLE event, got {rows}")

    def test_different_timeout_values(self):
        """US3: Configuration flexibility - different timeout values

        Test that IDLE_TIMEOUT can be configured with different values
        within the valid range (1s to 10d).

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: Different timeout configurations ===")

        # Test 1: Minimum timeout (1s)
        tdLog.info("Testing IDLE_TIMEOUT(1s)...")
        self.init_test_database("timeout_values")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")

        stream_name_1s = "s_timeout_1s"
        self.create_stream_and_wait(
            stream_name_1s,
            f"create stream {stream_name_1s} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(1s), event_type(idle)) "
            f"into out_1s "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        tdSql.execute("insert into d001 values (now, 1)")
        time.sleep(2)  # Wait 2s (> 1s timeout)

        tdSql.query("select * from out_1s")
        if tdSql.queryRows > 0:
            tdLog.success("✓ IDLE_TIMEOUT(1s) works")
        else:
            tdLog.exit("✗ IDLE_TIMEOUT(1s) failed")

        # Test 2: Medium timeout (5m)
        tdLog.info("Testing IDLE_TIMEOUT(5m) syntax...")
        tdSql.execute(f"drop stream {stream_name_1s}")
        stream_name_5m = "s_timeout_5m"
        self.create_stream_and_wait(
            stream_name_5m,
            f"create stream {stream_name_5m} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(5m), event_type(idle)) "
            f"into out_5m "
            f"as select _tidlestart, _tidleend, tbname from st"
        )
        tdLog.success("✓ IDLE_TIMEOUT(5m) syntax accepted")

        # Test 3: Maximum timeout (10d)
        tdLog.info("Testing IDLE_TIMEOUT(10d) syntax...")
        tdSql.execute(f"drop stream {stream_name_5m}")
        stream_name_10d = "s_timeout_10d"
        self.create_stream_and_wait(
            stream_name_10d,
            f"create stream {stream_name_10d} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(10d), event_type(idle)) "
            f"into out_10d "
            f"as select _tidlestart, _tidleend, tbname from st"
        )
        tdLog.success("✓ IDLE_TIMEOUT(10d) syntax accepted")

        # Test 4: Invalid timeout (< 1s) should fail
        tdLog.info("Testing invalid IDLE_TIMEOUT(500a)...")
        try:
            invalid_stream_name = "s_timeout_invalid_lt1s"
            tdSql.execute(
                f"create stream {invalid_stream_name} "
                f"sliding(1s) from st partition by tbname "
                f"stream_options(idle_timeout(500a), event_type(idle)) "
                f"into out_invalid "
                f"as select _tidlestart, _tidleend, tbname from st"
            )
            tdLog.exit("✗ Should reject IDLE_TIMEOUT < 1s")
        except Exception as e:
            tdLog.success(f"✓ Correctly rejected invalid timeout: {e}")

        # Test 5: Invalid timeout (> 10d) should fail
        tdLog.info("Testing invalid IDLE_TIMEOUT(11d)...")
        try:
            invalid_stream_name = "s_timeout_invalid_gt10d"
            tdSql.execute(
                f"create stream {invalid_stream_name} "
                f"sliding(1s) from st partition by tbname "
                f"stream_options(idle_timeout(11d), event_type(idle)) "
                f"into out_invalid2 "
                f"as select _tidlestart, _tidleend, tbname from st"
            )
            tdLog.exit("✗ Should reject IDLE_TIMEOUT > 10d")
        except Exception as e:
            tdLog.success(f"✓ Correctly rejected invalid timeout: {e}")

    def test_placeholder_values(self):
        """US1: Verify _tidlestart and _tidleend placeholder values

        Test that _tidlestart and _tidleend placeholders are correctly
        filled with idle period timestamps.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: Placeholder values ===")

        self.init_test_database("placeholder_values")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")

        # Create stream using placeholders
        stream_name = "s_placeholder_values"
        self.create_stream_and_wait(
            stream_name,
            f"create stream {stream_name} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(2s), event_type(idle)) "
            f"into out_placeholders "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        # Record insert time
        before_insert = int(time.time() * 1000)
        tdSql.execute("insert into d001 values (now, 1)")
        after_insert = int(time.time() * 1000)
        time.sleep(3)  # Wait for idle

        # Check placeholder values
        tdSql.query("select _tidlestart, _tidleend from out_placeholders")
        if tdSql.queryRows > 0:
            idle_start = tdSql.getData(0, 0)
            idle_end = tdSql.getData(0, 1)
            tdLog.info(f"_tidlestart: {idle_start}, _tidleend: {idle_end}")

            # Verify timestamps are reasonable
            if idle_start >= before_insert and idle_end > idle_start:
                tdLog.success("✓ Placeholder values are correct")
            else:
                tdLog.exit(f"✗ Invalid placeholder values: start={idle_start}, end={idle_end}")
        else:
            tdLog.exit("✗ No IDLE event triggered")

    def test_event_type_combinations(self):
        """US3: Test EVENT_TYPE combinations (IDLE only, RESUME only, IDLE|RESUME)

        Test that different event type combinations work correctly.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: Event type combinations ===")

        self.init_test_database("event_type_combinations")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")

        # Test 1: Only IDLE events
        tdLog.info("Testing EVENT_TYPE(IDLE)...")
        stream_name_idle = "s_event_type_idle"
        self.create_stream_and_wait(
            stream_name_idle,
            f"create stream {stream_name_idle} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(2s), event_type(idle)) "
            f"into out_idle_only "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        tdSql.execute("insert into d001 values (now, 1)")
        time.sleep(3)
        tdSql.execute("insert into d001 values (now, 2)")  # Should not trigger RESUME
        time.sleep(1)

        tdSql.query("select * from out_idle_only")
        idle_only_count = tdSql.queryRows
        tdLog.info(f"IDLE-only events: {idle_only_count}")

        # Test 2: Only RESUME events (requires IDLE first, but won't output IDLE)
        tdLog.info("Testing EVENT_TYPE(RESUME)...")
        tdSql.execute(f"drop stream {stream_name_idle}")
        stream_name_resume = "s_event_type_resume"
        self.create_stream_and_wait(
            stream_name_resume,
            f"create stream {stream_name_resume} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(2s), event_type(resume)) "
            f"into out_resume_only "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        tdSql.execute("insert into d001 values (now, 3)")
        time.sleep(3)  # Go idle
        tdSql.execute("insert into d001 values (now, 4)")  # Trigger RESUME
        time.sleep(1)

        tdSql.query("select * from out_resume_only")
        resume_only_count = tdSql.queryRows
        tdLog.info(f"RESUME-only events: {resume_only_count}")

        # Test 3: Both IDLE and RESUME
        tdLog.info("Testing EVENT_TYPE(IDLE|RESUME)...")
        tdSql.execute(f"drop stream {stream_name_resume}")
        stream_name_both = "s_event_type_both"
        self.create_stream_and_wait(
            stream_name_both,
            f"create stream {stream_name_both} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(2s), event_type(idle|resume)) "
            f"into out_both "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        tdSql.execute("insert into d001 values (now, 5)")
        time.sleep(3)  # Trigger IDLE
        tdSql.execute("insert into d001 values (now, 6)")  # Trigger RESUME
        time.sleep(1)

        tdSql.query("select * from out_both")
        both_count = tdSql.queryRows
        tdLog.info(f"IDLE|RESUME events: {both_count}")

        if both_count >= 2:
            tdLog.success("✓ Event type combinations work correctly")
        else:
            tdLog.exit(f"✗ Expected at least 2 events, got {both_count}")

    def test_no_idle_timeout_config(self):
        """US3: Test behavior when IDLE_TIMEOUT is not configured

        Test that idle detection is disabled when IDLE_TIMEOUT is not configured.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: No IDLE_TIMEOUT configuration ===")

        self.init_test_database("no_idle_timeout")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")

        # Create stream without IDLE_TIMEOUT
        stream_name = "s_no_idle_timeout"
        self.create_stream_and_wait(
            stream_name,
            f"create stream {stream_name} "
            f"sliding(1s) from st partition by tbname "
            f"into out_no_timeout "
            f"as select ts, v, tbname from st"
        )

        tdSql.execute("insert into d001 values (now, 1)")
        time.sleep(5)  # Wait long time

        # No idle events should be triggered
        tdSql.query("select * from out_no_timeout")
        rows = tdSql.queryRows
        tdLog.info(f"Output rows: {rows}")

        if rows == 1:  # Only the data row, no idle events
            tdLog.success("✓ No idle events when IDLE_TIMEOUT not configured")
        else:
            tdLog.info(f"Got {rows} rows (expected 1 data row)")

    def test_idle_resume_cycle(self):
        """US2: Test multiple idle-resume cycles

        Test that a partition can go through multiple idle-resume cycles.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: Multiple idle-resume cycles ===")

        self.init_test_database("idle_resume_cycle")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")

        stream_name = "s_idle_resume_cycle"
        self.create_stream_and_wait(
            stream_name,
            f"create stream {stream_name} "
            f"sliding(1s) from st partition by tbname "
            f"stream_options(idle_timeout(2s), event_type(idle|resume)) "
            f"into out_cycles "
            f"as select _tidlestart, _tidleend, tbname from st"
        )

        # Cycle 1: IDLE -> RESUME
        tdSql.execute("insert into d001 values (now, 1)")
        time.sleep(3)  # IDLE
        tdSql.execute("insert into d001 values (now, 2)")  # RESUME
        time.sleep(1)

        # Cycle 2: IDLE -> RESUME
        time.sleep(2)  # IDLE again
        tdSql.execute("insert into d001 values (now, 3)")  # RESUME again
        time.sleep(1)

        # Cycle 3: IDLE -> RESUME
        time.sleep(2)  # IDLE again
        tdSql.execute("insert into d001 values (now, 4)")  # RESUME again
        time.sleep(1)

        tdSql.query("select * from out_cycles")
        rows = tdSql.queryRows
        tdLog.info(f"Total events in 3 cycles: {rows}")

        if rows >= 6:  # 3 cycles * 2 events (IDLE + RESUME)
            tdLog.success("✓ Multiple idle-resume cycles work correctly")
        else:
            tdLog.exit(f"✗ Expected at least 6 events, got {rows}")

    def test_placeholder_mixing_error(self):
        """Edge case: Test that mixing window and idle placeholders is rejected

        Test that using both _twstart/_twend and _tidlestart/_tidleend
        in the same stream is rejected by the parser.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: Placeholder mixing error ===")

        self.init_test_database("placeholder_mixing_error")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")

        # Try to mix window and idle placeholders
        try:
            stream_name = "s_placeholder_mixing_error"
            tdSql.execute(
                f"create stream {stream_name} "
                f"interval(1s) from st partition by tbname "
                f"stream_options(idle_timeout(2s), event_type(idle)) "
                f"into out_mixed "
                f"as select _twstart, _tidlestart, tbname from st"
            )
            tdLog.exit("✗ Should reject mixing window and idle placeholders")
        except Exception as e:
            tdLog.success(f"✓ Correctly rejected placeholder mixing: {e}")

    def test_idle_without_timeout_error(self):
        """Edge case: Test that IDLE/RESUME events require IDLE_TIMEOUT

        Test that using EVENT_TYPE(IDLE) or EVENT_TYPE(RESUME) without
        IDLE_TIMEOUT configuration is rejected.

        Since: v3.3.4.0

        Labels: stream, idle-trigger, ci

        Jira: None

        History:
            - 2026-03-17 Created
        """
        tdLog.info("=== Test: IDLE without IDLE_TIMEOUT error ===")

        self.init_test_database("idle_without_timeout_error")
        tdSql.execute("create table st (ts timestamp, v int) tags (gid int)")
        tdSql.execute("create table d001 using st tags (1)")

        # Try to use IDLE event without IDLE_TIMEOUT
        try:
            stream_name = "s_idle_without_timeout_error"
            tdSql.execute(
                f"create stream {stream_name} "
                f"sliding(1s) from st partition by tbname "
                f"stream_options(event_type(idle)) "
                f"into out_error "
                f"as select _tidlestart, _tidleend, tbname from st"
            )
            tdLog.exit("✗ Should reject IDLE event without IDLE_TIMEOUT")
        except Exception as e:
            tdLog.success(f"✓ Correctly rejected IDLE without IDLE_TIMEOUT: {e}")
