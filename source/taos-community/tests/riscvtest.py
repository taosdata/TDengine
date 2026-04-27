#!/usr/bin/env python3
"""
RISC-V TDengine Test Runner

A Python rewrite of riscvtest.sh that consolidates:
  - All enabled test cases from riscvtest.sh (1212 cases)
  - All disabled/commented-out cases (69 cases, marked as "disabled")
  - All failed/timeout cases from riscv-report.md (13 cases, marked as "known_fail")

Usage:
    python3 riscvtest.py [options]

Options:
    -t, --timeout SEC       Per-case timeout in seconds (default: 300)
    -b, --bin-dir PATH      Path to taosd/taos binaries
    -c, --cfg-extra PATH    Extra taos.cfg file to merge
    -s, --start-from CASE   Skip cases until this substring is found
    --only-failed           Only run known-failed + disabled cases
    --only-disabled         Only run disabled cases
    --only-enabled          Only run enabled cases (skip disabled)
    --dry-run               Print cases without executing
    --json-report PATH      Write JSON report to file
    -j, --jobs N            Parallel workers (default: 1, serial)
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional


# ─── Case Status ────────────────────────────────────────────────────────────

class CaseTag(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"       # commented out in riscvtest.sh
    KNOWN_FAIL = "known_fail"   # failed/timeout in riscv-report.md


class RunResult(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    TIMEOUT = "TIMEOUT"
    SKIPPED = "SKIPPED"


# ─── Data Structures ────────────────────────────────────────────────────────

@dataclass
class TestCase:
    subfolder: str          # e.g. "system-test", "army", "develop-test"
    command: str            # e.g. "python3 ./test.py -f 0-others/xxx.py -N 3"
    tag: CaseTag = CaseTag.ENABLED
    disable_reason: str = ""
    fail_reason: str = ""   # from riscv-report.md analysis


@dataclass
class CaseResult:
    subfolder: str
    command: str
    tag: str
    result: str
    rc: int = 0
    duration_s: float = 0.0
    start_time: str = ""
    end_time: str = ""


# ─── Known Failed Cases from riscv-report.md ────────────────────────────────
# 8 FAIL + 4 TIMEOUT + 1 NOT_EXECUTED = 13 cases

KNOWN_FAIL_CASES = {
    # --- 8 FAIL ---
    ("system-test", "python3 ./test.py -f 0-others/retention_test.py"):
        "AttributeError: TDLog has no attribute 'error', fix: tdLog.error -> tdLog.exit",
    ("system-test", "python3 ./test.py -f 0-others/show_disk_usage_multilevel.py"):
        "AttributeError: TDLog has no attribute 'error', fix: tdLog.error -> tdLog.exit",
    ("system-test", "python3 ./test.py -f 0-others/wal_level_skip.py"):
        "WAL level switch: Table does not exist after 20 retries",
    ("system-test", "python3 ./test.py -f 2-query/td-32548.py"):
        "last_row() returns abnormal value, suspected concurrency timing issue",
    ("system-test", "python3 ./test.py -f 2-query/timetruncate.py -Q 4"):
        "WebSocket mode (-Q 4) failure, -Q 1/2/3/R all pass",
    ("system-test", "python3 ./test.py -f 7-tmq/tmq_connection.py"):
        "show connections expects 2 but got 34, test isolation issue",
    ("army", "python3 ./test.py -f vtable/test_vtable_query.py"):
        "Float precision: RISC-V (-163781.01) vs x86 answer (-163685.41)",
    ("army", "python3 ./test.py -f vtable/test_vtable_query_cross_db.py"):
        "Float precision: cross-db scenario, same root cause as vtable_query",
    # --- 4 TIMEOUT ---
    ("system-test", "python3 ./test.py -f 0-others/compatibility_rolling_upgrade.py -N 3"):
        "TIMEOUT: 3-node rolling upgrade, RISC-V startup ~5x slower than x86",
    ("army", "python3 ./test.py -f query/function/test_func_paramnum.py"):
        "TIMEOUT: test logic passed but process hung in teardown",
    ("system-test", "python3 ./test.py -f 8-stream/checkpoint_info.py -N 4"):
        "TIMEOUT: hardcoded time.sleep(200) + 4-node startup exceeds 480s",
    ("system-test", "python3 ./test.py -f 0-others/taosd_audit.py"):
        "TIMEOUT: infinite loop - threadisExit never set on RISC-V",
    # --- 1 NOT EXECUTED ---
    ("army", "python3 ./test.py -f storage/s3/s3Basic.py -N 3"):
        "NOT_EXECUTED: S3 storage backend not configured on RISC-V",
}


# ─── Disabled Cases (commented out in riscvtest.sh) ─────────────────────────

DISABLED_CASES = [
    # stream
    ("army",        "python3 ./test.py -f stream/test_stream_vtable.py",            "max retries exceeded"),
    ("system-test", "python3 ./test.py -f 8-stream/stream_multi_agg.py",            "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/stream_basic.py",                "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/scalar_function.py",             "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/at_once_interval.py",            "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/at_once_session.py",             "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/at_once_state_window.py",        "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/window_close_interval.py",       "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/window_close_session.py",        "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/window_close_state_window.py",   "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/max_delay_interval.py",          "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/max_delay_session.py",           "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/at_once_interval_ext.py",        "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/max_delay_interval_ext.py",      "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/window_close_session_ext.py",    "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/partition_interval.py",          "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/state_window_case.py",           "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/snode_restart_with_checkpoint.py -N 4", "time limit exceeded"),
    ("system-test", "python3 ./test.py -f 8-stream/force_window_close_interp.py",   "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/force_window_close_interval.py", "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 8-stream/checkpoint_info2.py -N 4",       "no-fix retries exhausted"),
    # decimal
    ("system-test", "python3 ./test.py -f 2-query/decimal3.py",                     "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/decimal3.py -Q 4",                "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/decimal3.py -Q 3",                "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/decimal3.py -Q 2",                "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/decimal3.py -Q 1",                "no-fix retries exhausted"),
    # tmq
    ("system-test", "python3 ./test.py -f 7-tmq/tmqDropStb.py",                     "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmqParamsTest.py -R",               "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmqMaxGroupIds.py",                 "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmqDropConsumer.py",                "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmqAutoCreateTbl.py",               "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmqDelete-multiCtb.py -N 3 -n 3",   "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmqUdf.py",                         "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot0.py",       "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot1.py",       "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/tmq_taosx.py",                      "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 7-tmq/walRemoveLog.py -N 3",              "no-fix retries exhausted"),
    # query
    ("system-test", "python3 ./test.py -f 2-query/insert_null_none.py -R",          "time limit exceeded"),
    ("system-test", "python3 ./test.py -f 2-query/agg_group_NotReturnValue.py -Q 3", "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/large_data.py",                   "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/char_length.py",                  "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/char_length.py -R",               "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/char_length.py -Q 2",             "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/char_length.py -Q 3",             "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/char_length.py -Q 4",             "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/cols_function.py",                "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/cols_function.py -Q 2",           "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/cols_function.py -Q 3",           "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/cols_function.py -Q 4",           "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 2-query/test_window_true_for.py",         "no-fix retries exhausted"),
    # insert
    ("system-test", "python3 ./test.py -f 1-insert/database_pre_suf.py",            "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 1-insert/alter_database.py",              "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 1-insert/rowlength64k_benchmark.py",      "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 1-insert/precisionUS.py",                 "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 1-insert/precisionNS.py",                 "no-fix retries exhausted"),
    # 0-others
    ("system-test", "python3 ./test.py -f 0-others/taosdlog.py",                    "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/udfTest.py",                     "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/udf_create.py",                  "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/udf_restart_taosd.py",           "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/udf_cfg1.py",                    "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/udf_cfg2.py",                    "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/test_show_disk_usage.py",        "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/tag_index_basic.py",             "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/kill_balance_leader.py -N 3",    "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/dumpsdb.py",                     "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/grant.py",                       "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/compatibility_rolling_upgrade_all.py -N 3", "no-fix retries exhausted"),
    ("system-test", "python3 ./test.py -f 0-others/compatibility.py",               "no-fix retries exhausted"),
    # tools
    ("army",        "python3 ./test.py -f tools/taosdump/ws/taosdumpRetry.py -B",   "max retries exceeded"),
]


# ─── Build Full Case List ───────────────────────────────────────────────────

def build_case_list() -> list[TestCase]:
    """
    Build the complete test case list by parsing riscvtest.sh for enabled cases,
    then appending disabled cases and annotating known failures.
    """
    cases: list[TestCase] = []
    seen: set[tuple[str, str]] = set()

    # 1) Parse enabled cases from riscvtest.sh (embedded inline)
    for subfolder, cmd in ENABLED_CASES:
        key = (subfolder, cmd)
        if key in seen:
            continue
        seen.add(key)
        tag = CaseTag.ENABLED
        fail_reason = ""
        if key in KNOWN_FAIL_CASES:
            tag = CaseTag.KNOWN_FAIL
            fail_reason = KNOWN_FAIL_CASES[key]
        cases.append(TestCase(
            subfolder=subfolder, command=cmd, tag=tag, fail_reason=fail_reason,
        ))

    # 2) Append disabled cases
    for subfolder, cmd, reason in DISABLED_CASES:
        key = (subfolder, cmd)
        if key in seen:
            continue
        seen.add(key)
        cases.append(TestCase(
            subfolder=subfolder, command=cmd,
            tag=CaseTag.DISABLED, disable_reason=reason,
        ))

    return cases


# ─── Runner ─────────────────────────────────────────────────────────────────

def run_one_case(
    base_dir: Path,
    case: TestCase,
    timeout: int,
    log_file,
) -> CaseResult:
    """Execute a single test case and return the result."""
    start = datetime.now()
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    header = f"[START][{start_str}] {case.subfolder}: {case.command}"
    print(header)
    log_file.write(header + "\n")
    log_file.flush()

    work_dir = base_dir / case.subfolder
    try:
        proc = subprocess.run(
            case.command,
            shell=True,
            cwd=str(work_dir),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        rc = 124  # match bash `timeout` exit code
    except Exception as e:
        log_file.write(f"Exception: {e}\n")
        rc = 1

    end = datetime.now()
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    duration = (end - start).total_seconds()

    if rc == 0:
        result = RunResult.PASS
        tag_str = f"[PASS ][{end_str}]"
    elif rc == 124:
        result = RunResult.TIMEOUT
        tag_str = f"[TIMEOUT][{end_str}] >{timeout}s"
    else:
        result = RunResult.FAIL
        tag_str = f"[FAIL ][{end_str}] rc={rc}"

    line = f"{tag_str} {case.subfolder}: {case.command}"
    print(line)
    log_file.write(line + "\n\n")
    log_file.flush()

    return CaseResult(
        subfolder=case.subfolder,
        command=case.command,
        tag=case.tag.value,
        result=result.value,
        rc=rc,
        duration_s=round(duration, 1),
        start_time=start_str,
        end_time=end_str,
    )


def print_summary(results: list[CaseResult], wall_start: datetime):
    """Print a summary table."""
    total = len(results)
    passed = sum(1 for r in results if r.result == RunResult.PASS.value)
    failed = sum(1 for r in results if r.result == RunResult.FAIL.value)
    timed_out = sum(1 for r in results if r.result == RunResult.TIMEOUT.value)
    skipped = sum(1 for r in results if r.result == RunResult.SKIPPED.value)
    wall_time = (datetime.now() - wall_start).total_seconds()

    print("\n" + "=" * 70)
    print(f"  TOTAL: {total}  |  PASS: {passed}  |  FAIL: {failed}  "
          f"|  TIMEOUT: {timed_out}  |  SKIPPED: {skipped}")
    if total - skipped > 0:
        rate = passed / (total - skipped) * 100
        print(f"  Pass rate: {rate:.1f}%  ({passed}/{total - skipped})")
    print(f"  Wall time: {wall_time:.0f}s ({wall_time/3600:.1f}h)")
    print("=" * 70)

    # Print failures
    failures = [r for r in results if r.result in (RunResult.FAIL.value, RunResult.TIMEOUT.value)]
    if failures:
        print(f"\n  Failed/Timeout cases ({len(failures)}):")
        for r in failures:
            print(f"    [{r.result:7s}] {r.subfolder}: {r.command}")
    print()


# ─── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RISC-V TDengine Test Runner")
    parser.add_argument("-t", "--timeout", type=int, default=300,
                        help="Per-case timeout in seconds (default: 300)")
    parser.add_argument("-b", "--bin-dir", default=None,
                        help="Path to taosd/taos binaries")
    parser.add_argument("-c", "--cfg-extra", default=None,
                        help="Extra taos.cfg file path")
    parser.add_argument("-s", "--start-from", default=None,
                        help="Skip until this case substring is matched")
    parser.add_argument("--only-failed", action="store_true",
                        help="Only run known-failed + disabled cases")
    parser.add_argument("--only-disabled", action="store_true",
                        help="Only run disabled cases")
    parser.add_argument("--only-enabled", action="store_true",
                        help="Only run enabled cases (skip disabled)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print cases without executing")
    parser.add_argument("--json-report", default=None,
                        help="Write JSON report to file")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent

    # Validate bin-dir
    if args.bin_dir:
        bin_path = Path(args.bin_dir)
        if not (bin_path / "taosd").exists():
            print(f"ERROR: -b '{args.bin_dir}' does not contain 'taosd'", file=sys.stderr)
            sys.exit(1)
        os.environ["TAOS_BIN_DIR"] = str(bin_path.resolve())

    if args.cfg_extra:
        cfg_path = Path(args.cfg_extra)
        if not cfg_path.is_file():
            print(f"ERROR: -c '{args.cfg_extra}' not readable", file=sys.stderr)
            sys.exit(1)
        os.environ["TAOS_CFG_EXTRA"] = str(cfg_path.resolve())

    # Build case list
    all_cases = build_case_list()

    # Filter
    if args.only_failed:
        cases = [c for c in all_cases if c.tag in (CaseTag.KNOWN_FAIL, CaseTag.DISABLED)]
    elif args.only_disabled:
        cases = [c for c in all_cases if c.tag == CaseTag.DISABLED]
    elif args.only_enabled:
        cases = [c for c in all_cases if c.tag == CaseTag.ENABLED]
    else:
        cases = all_cases

    # Start-from
    if args.start_from:
        skip = True
        filtered = []
        for c in cases:
            if skip and args.start_from in c.command:
                skip = False
            if not skip:
                filtered.append(c)
        cases = filtered

    # Dry run
    if args.dry_run:
        enabled = [c for c in cases if c.tag == CaseTag.ENABLED]
        known_fail = [c for c in cases if c.tag == CaseTag.KNOWN_FAIL]
        disabled = [c for c in cases if c.tag == CaseTag.DISABLED]
        print(f"Total: {len(cases)} (enabled={len(enabled)}, "
              f"known_fail={len(known_fail)}, disabled={len(disabled)})\n")
        for c in cases:
            marker = {"enabled": "  ", "known_fail": "! ", "disabled": "# "}[c.tag.value]
            print(f"{marker}{c.subfolder:15s} {c.command}")
            if c.fail_reason:
                print(f"    -> {c.fail_reason}")
            if c.disable_reason:
                print(f"    -> DISABLED: {c.disable_reason}")
        return

    # Run
    log_path = base_dir / "riscvtest.log"
    wall_start = datetime.now()

    print(f"=== riscvtest.py start at {wall_start.strftime('%Y-%m-%d %H:%M:%S')} ===")
    print(f"Python: {sys.version}")
    print(f"Case timeout: {args.timeout}s")
    print(f"Binary dir:   {os.environ.get('TAOS_BIN_DIR', '(auto-detect)')}")
    print(f"Total cases:  {len(cases)}")
    print()

    results: list[CaseResult] = []

    with open(log_path, "a") as log_file:
        log_file.write(f"\n=== riscvtest.py start at "
                       f"{wall_start.strftime('%Y-%m-%d %H:%M:%S')} ===\n")

        for i, case in enumerate(cases, 1):
            print(f"\n--- [{i}/{len(cases)}] tag={case.tag.value} ---")

            if case.tag == CaseTag.DISABLED and not (args.only_failed or args.only_disabled):
                # In default mode, skip disabled cases
                r = CaseResult(
                    subfolder=case.subfolder, command=case.command,
                    tag=case.tag.value, result=RunResult.SKIPPED.value,
                )
                results.append(r)
                print(f"[SKIP ] {case.subfolder}: {case.command}")
                print(f"        reason: {case.disable_reason}")
                continue

            r = run_one_case(base_dir, case, args.timeout, log_file)
            results.append(r)

    print_summary(results, wall_start)

    # JSON report
    if args.json_report:
        report = {
            "start_time": wall_start.isoformat(),
            "end_time": datetime.now().isoformat(),
            "timeout": args.timeout,
            "summary": {
                "total": len(results),
                "pass": sum(1 for r in results if r.result == "PASS"),
                "fail": sum(1 for r in results if r.result == "FAIL"),
                "timeout": sum(1 for r in results if r.result == "TIMEOUT"),
                "skipped": sum(1 for r in results if r.result == "SKIPPED"),
            },
            "results": [asdict(r) for r in results],
        }
        with open(args.json_report, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"JSON report written to {args.json_report}")


# ─── Enabled Cases (extracted from riscvtest.sh) ────────────────────────────
# 1212 enabled run_case entries (preserving original order)

ENABLED_CASES = [
    ("system-test", "python3 ./test.py -f 0-others/compatibility_rolling_upgrade.py -N 3"),
    ("army", "python3 ./test.py -f multi-level/mlevel_basic.py -N 3 -L 3 -D 2"),
    ("army", "python3 ./test.py -f db-encrypt/basic.py -N 3 -M 3"),
    ("army", "python3 ./test.py -f cluster/arbitrator.py -N 3"),
    ("army", "python3 ./test.py -f cluster/arbitrator_restart.py -N 3"),
    ("army", "python3 ./test.py -f storage/s3/s3Basic.py -N 3"),
    ("army", "python3 ./test.py -f cluster/snapshot.py -N 3 -L 3 -D 2"),
    ("army", "python3 ./test.py -f vtable/test_vtable_create.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_alter.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_drop.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_meta.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_auth_create.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_auth_select.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_auth_alter_drop.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_auth_alter_drop_child.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_same_db_stb.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_same_db_stb_window.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_same_db_stb_group.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_same_reference_col.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_cross_db.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_cross_db_stb.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_cross_db_stb_window.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_cross_db_stb_group.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_after_alter.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_after_drop_origin_table.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_query_after_alter_origin_table.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_schema_is_old.py"),
    ("army", "python3 ./test.py -f vtable/test_vtable_join.py"),
    ("army", "python3 ./test.py -f query/decimal/test_TS6333.py"),
    ("army", "python3 ./test.py -f query/function/test_func_elapsed.py"),
    ("army", "python3 ./test.py -f query/function/test_function.py"),
    ("army", "python3 ./test.py -f query/function/test_selection_function_with_json.py"),
    ("army", "python3 ./test.py -f query/function/test_func_paramnum.py"),
    ("army", "python3 ./test.py -f query/function/test_percentile.py"),
    ("army", "python3 ./test.py -f query/function/test_resinfo.py"),
    ("army", "python3 ./test.py -f query/function/test_interp.py"),
    ("army", "python3 ./test.py -f query/function/test_interval.py"),
    ("army", "python3 ./test.py -f query/function/test_interval_diff_tz.py"),
    ("army", "python3 ./test.py -f query/function/concat.py"),
    ("army", "python3 ./test.py -f query/function/cast.py"),
    ("army", "python3 ./test.py -f query/test_join.py"),
    ("army", "python3 ./test.py -f query/test_join_const.py"),
    ("army", "python3 ./test.py -f query/test_compare.py"),
    ("army", "python3 ./test.py -f query/test_case_when.py"),
    ("army", "python3 ./test.py -f insert/test_column_tag_boundary.py"),
    ("army", "python3 ./test.py -f query/fill/fill_desc.py -N 3 -L 3 -D 2"),
    ("army", "python3 ./test.py -f query/fill/fill_null.py"),
    ("army", "python3 ./test.py -f cluster/test_drop_table_by_uid.py -N 3"),
    ("army", "python3 ./test.py -f cluster/incSnapshot.py -N 3"),
    ("army", "python3 ./test.py -f cluster/clusterBasic.py -N 5"),
    ("army", "python3 ./test.py -f cluster/tsdbSnapshot.py -N 3 -M 3"),
    ("army", "python3 ./test.py -f cluster/strongPassword.py"),
    ("army", "python3 ./test.py -f query/query_basic.py -N 3"),
    ("army", "python3 ./test.py -f query/accuracy/test_query_accuracy.py"),
    ("army", "python3 ./test.py -f query/accuracy/test_ts5400.py"),
    ("army", "python3 ./test.py -f query/accuracy/test_having.py"),
    ("army", "python3 ./test.py -f insert/insert_basic.py -N 3"),
    ("army", "python3 ./test.py -f insert/auto_create_insert.py"),
    ("army", "python3 ./test.py -f cluster/splitVgroupByLearner.py -N 3"),
    ("army", "python3 ./test.py -f authorith/authBasic.py -N 3"),
    ("army", "python3 ./test.py -f cmdline/fullopt.py"),
]


# The remaining ~1150 enabled cases are too many to inline manually.
# We auto-extract them from riscvtest.sh at import time if it exists alongside this script.

def _load_enabled_from_sh():
    """Parse riscvtest.sh to extract all enabled run_case lines."""
    import re
    sh_path = Path(__file__).resolve().parent / "riscvtest.sh"
    if not sh_path.exists():
        return
    extra = []
    existing = {(s, c) for s, c in ENABLED_CASES}
    with open(sh_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("#") or not line.startswith("run_case "):
                continue
            m = re.match(r"run_case\s+(\S+)\s+(.*)", line)
            if m:
                subfolder = m.group(1)
                cmd = m.group(2).strip()
                key = (subfolder, cmd)
                if key not in existing:
                    extra.append(key)
                    existing.add(key)
    ENABLED_CASES.extend(extra)

_load_enabled_from_sh()


if __name__ == "__main__":
    main()
