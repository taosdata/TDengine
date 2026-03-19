#!/usr/bin/env python3
# filepath: hot_update/run/reporter.py
#
# Structured test output reporter.
# Provides START / DONE banners, check-item lines, and SUMMARY blocks.

import sys
import os
import time
from datetime import datetime

W = 72   # total line width


def _now():
    return datetime.now().strftime("%H:%M:%S")


def _bar(char="═"):
    return char * W


class Reporter:
    """
    Minimal, structured output helper.

    Usage
    -----
    r = Reporter()
    r.summary_start(...)           # opening SUMMARY block

    r.step_start("Cluster setup")
    ...do work...
    r.step_done("Cluster setup")

    r.check("Write latency ≤ 2s", passed=True, detail="max=0.12s")
    r.summary_end(passed=True, items=[...])
    """

    # ------------------------------------------------------------------
    # Opening SUMMARY
    # ------------------------------------------------------------------

    def summary_start(self, from_ver: str, to_ver: str, fqdn: str,
                      dnode_count: int, mnode_count: int,
                      subtables: int, rows_per_table: int,
                      verify_window: int,
                      check_sysinfo: bool = False,
                      gen_whitelist=None):
        print(_bar("═"))
        print(f"  TDengine Rolling Upgrade Test")
        print(_bar("─"))
        print(f"  From version : {from_ver}")
        print(f"  To version   : {to_ver}")
        print(f"  Host (FQDN)  : {fqdn}")
        print(f"  Cluster      : {dnode_count} DNODEs / {mnode_count} MNODEs / 3-replica")
        print(f"  Dataset      : {subtables} subtables × {rows_per_table:,} rows")
        print(f"  Verify window: {verify_window}s")
        if gen_whitelist is not None:
            fname = gen_whitelist if isinstance(gen_whitelist, str) else "(auto)"
            print(f"  Gen-whitelist: {fname}")
        elif check_sysinfo:
            print(f"  SysInfo check: enabled")
        print(f"  Started at   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(_bar("═"))
        print()

    # ------------------------------------------------------------------
    # Step START / DONE
    # ------------------------------------------------------------------

    def step_start(self, name: str):
        ts = _now()
        label = f"  [{ts}] {name}"
        print(f"{label}")
        print(f"  {'─' * (W - 2)}  START")
        sys.stdout.flush()
        self._step_t0 = time.time()

    def step_done(self, name: str):
        elapsed = time.time() - getattr(self, "_step_t0", time.time())
        ts = _now()
        print(f"  {'─' * (W - 2)}  DONE  ({elapsed:.1f}s)")
        print()
        sys.stdout.flush()

    # ------------------------------------------------------------------
    # Upgrade node progress
    # ------------------------------------------------------------------

    def node_upgrade(self, index: int, total: int, order: list):
        print(f"  Upgrade order : {order}")
        print(f"  Upgrading DNode {index}  ({index}/{total}) ...")
        sys.stdout.flush()

    def node_upgrade_done(self, index: int):
        print(f"  DNode {index} .......................................... [upgraded]")
        sys.stdout.flush()

    # ------------------------------------------------------------------
    # Verification check items
    # ------------------------------------------------------------------

    def check(self, label: str, passed: bool, detail: str = ""):
        FILL = 55
        dots = "." * max(4, FILL - len(label))
        status = "[passed]" if passed else "[FAILED]"
        detail_str = f"  {detail}" if detail else ""
        print(f"  {label} {dots} {status}{detail_str}")
        sys.stdout.flush()

    # ------------------------------------------------------------------
    # Inline info / warning (kept minimal during test run)
    # ------------------------------------------------------------------

    def info(self, msg: str):
        print(f"  {_now()}  {msg}")
        sys.stdout.flush()

    def warn(self, msg: str):
        print(f"  {_now()}  WARNING: {msg}", file=sys.stderr)
        sys.stdout.flush()

    def error(self, msg: str):
        print(f"  {_now()}  ERROR: {msg}", file=sys.stderr)
        sys.stdout.flush()

    # ------------------------------------------------------------------
    # Closing SUMMARY
    # ------------------------------------------------------------------

    def summary_end(self, passed: bool, checks: list, start_time: float,
                    write_max: float, query_max: float, sub_max: float,
                    base_path: str = "", log_dir: str = ""):
        """
        checks  : list of (label, passed, detail) tuples already printed via check()
        log_dir : TDengine log directory; printed on failure to help diagnosis
        """
        elapsed = time.time() - start_time
        m, s = divmod(int(elapsed), 60)

        print("\n")
        print(_bar("═"))
        print(f"  TDengine Rolling Upgrade Test  ─  SUMMARY")
        print(_bar("─"))
        for label, ok, detail in checks:
            FILL = 55
            dots = "." * max(4, FILL - len(label))
            status = "[passed]" if ok else "[FAILED]"
            detail_str = f"  {detail}" if detail else ""
            print(f"  {label} {dots} {status}{detail_str}")
        print(_bar("─"))
        print(f"  Write latency max   : {write_max:.3f}s")
        print(f"  Query latency max   : {query_max:.3f}s")
        print(f"  Subscribe gap max   : {sub_max:.3f}s")
        print(_bar("─"))
        result_str = "PASS ✓" if passed else "FAIL ✗"
        print(f"  Result  : {result_str}")
        if not passed:
            path = log_dir or (os.path.join(base_path, "dnode*/log") if base_path else "")
            if path:
                print(f"  Log dir : {path}")
        print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  "
              f"(elapsed {m}m {s}s)")
        print(_bar("═"))
        sys.stdout.flush()
