#!/usr/bin/env python3
# filepath: hot_update/resource/verifier.py
#
# Post-upgrade verifier.
# Observes the shared metrics dict for VERIFY_DURATION_S seconds and
# decides PASS or FAIL based on per-metric latency / gap thresholds.

import os
import sys
import time
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config
from server.clusterSetup import Logger


class VerifyResult:
    def __init__(self):
        self.passed          = True
        self.failures: list  = []
        self.write_max: float   = 0.0
        self.query_max: float   = 0.0
        self.subscribe_max: float = 0.0

    def fail(self, msg: str):
        self.passed = False
        self.failures.append(msg)

    def __str__(self):
        if self.passed:
            return (
                f"PASS  |  write_max={self.write_max:.3f}s  "
                f"query_max={self.query_max:.3f}s  "
                f"subscribe_gap_max={self.subscribe_max:.3f}s"
            )
        return "FAIL\n  " + "\n  ".join(self.failures)


class Verifier:
    """
    Uses the *shared metrics dict* (multiprocessing.Manager().dict()) that the
    writer / querier / subscriber processes update in real time.

    Call verify(metrics) AFTER rolling upgrade is complete.
    It will:
      1. Reset the per-window max values so only the observation period counts.
      2. Sleep for VERIFY_DURATION_S.
      3. Read final values and compare against thresholds.
    """

    def __init__(self, logger: Optional[Logger] = None):
        self.logger = logger or Logger()

    # ------------------------------------------------------------------

    def verify(self, metrics) -> VerifyResult:
        """
        `metrics` is a multiprocessing.Manager().dict() with keys described
        in run/main.py.  This method blocks for config.VERIFY_DURATION_S seconds.
        """
        result = VerifyResult()

        self.logger.info(
            f"Starting post-upgrade verification window "
            f"({config.VERIFY_DURATION_S}s) ..."
        )

        # ── Reset window maximums ──────────────────────────────────────
        metrics["write_window_max"]     = 0.0
        metrics["query_window_max"]     = 0.0
        metrics["subscribe_window_max"] = 0.0

        # ── Observe ────────────────────────────────────────────────────
        deadline = time.time() + config.VERIFY_DURATION_S
        while time.time() < deadline:
            # Early abort if any worker already signalled fatal failure
            if metrics.get("write_failed"):
                result.fail("Writer process reported a fatal failure (retry timeout).")
                return result
            if metrics.get("query_failed"):
                result.fail("Querier process reported a fatal failure (retry timeout).")
                return result
            if metrics.get("subscribe_failed"):
                result.fail("Subscriber process reported a fatal failure (retry timeout).")
                return result

            remaining = int(deadline - time.time())
            self.logger.info(
                f"  [{remaining:3d}s left]  "
                f"write={metrics.get('write_last_latency', 0):.3f}s  "
                f"query={metrics.get('query_last_latency', 0):.3f}s  "
                f"sub_gap={metrics.get('subscribe_last_gap', 0):.3f}s"
            )
            time.sleep(5)

        # ── Collect results ────────────────────────────────────────────
        result.write_max     = metrics.get("write_window_max",     0.0)
        result.query_max     = metrics.get("query_window_max",     0.0)
        result.subscribe_max = metrics.get("subscribe_window_max", 0.0)

        if metrics.get("write_failed"):
            result.fail("Writer process reported a fatal failure (retry timeout).")
        if metrics.get("query_failed"):
            result.fail("Querier process reported a fatal failure (retry timeout).")
        if metrics.get("subscribe_failed"):
            result.fail("Subscriber process reported a fatal failure (retry timeout).")

        if result.write_max > config.MAX_WRITE_LATENCY_S:
            result.fail(
                f"Write latency exceeded threshold: "
                f"{result.write_max:.3f}s > {config.MAX_WRITE_LATENCY_S}s"
            )
        if result.query_max > config.MAX_QUERY_LATENCY_S:
            result.fail(
                f"Query latency exceeded threshold: "
                f"{result.query_max:.3f}s > {config.MAX_QUERY_LATENCY_S}s"
            )
        if result.subscribe_max > config.MAX_SUBSCRIBE_GAP_S:
            result.fail(
                f"Subscribe gap exceeded threshold: "
                f"{result.subscribe_max:.3f}s > {config.MAX_SUBSCRIBE_GAP_S}s"
            )

        return result
