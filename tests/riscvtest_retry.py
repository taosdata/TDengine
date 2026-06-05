#!/usr/bin/env python3
"""
RISC-V TDengine 失败用例重测脚本

仅包含 riscv-report.md 中的失败(8) + 超时(4) + 未执行(1) + riscvtest.sh 中被注释掉的(69) 用例，
共 82 个 case。用于快速验证修复效果。

Usage:
    python3 riscvtest_retry.py                    # 运行全部 82 个
    python3 riscvtest_retry.py -t 600             # 设置超时 600s
    python3 riscvtest_retry.py --tag fail          # 只跑 report 中失败的 8 个
    python3 riscvtest_retry.py --tag timeout       # 只跑超时的 4 个
    python3 riscvtest_retry.py --tag disabled      # 只跑被注释掉的 69 个
    python3 riscvtest_retry.py --module stream     # 只跑 stream 模块
    python3 riscvtest_retry.py --dry-run           # 预览不执行
    python3 riscvtest_retry.py --json report.json  # 输出 JSON 报告
"""

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path


# ─── Case Definition ────────────────────────────────────────────────────────

@dataclass
class TestCase:
    subfolder: str       # "system-test" | "army"
    command: str         # "python3 ./test.py -f ..."
    tag: str             # "fail" | "timeout" | "not_exec" | "disabled"
    module: str          # 分类: stream / tmq / query / insert / udf / others / tools / vtable ...
    reason: str          # 失败/禁用原因


@dataclass
class CaseResult:
    subfolder: str
    command: str
    tag: str
    module: str
    result: str          # PASS / FAIL / TIMEOUT
    rc: int = 0
    duration_s: float = 0.0
    start_time: str = ""
    end_time: str = ""


# ─── 82 Cases ────────────────────────────────────────────────────────────────

CASES = [
    # ===================================================================
    # riscv-report.md: 8 FAIL
    # ===================================================================
    TestCase("system-test", "python3 ./test.py -f 0-others/retention_test.py",
             "fail", "others",
             "AttributeError: TDLog.error -> tdLog.exit 一行修复"),
    TestCase("system-test", "python3 ./test.py -f 0-others/show_disk_usage_multilevel.py",
             "fail", "others",
             "AttributeError: TDLog.error -> tdLog.exit 一行修复"),
    TestCase("system-test", "python3 ./test.py -f 0-others/wal_level_skip.py",
             "fail", "others",
             "WAL level 切换后 Table does not exist, 20 次重试超时"),
    TestCase("system-test", "python3 ./test.py -f 2-query/td-32548.py",
             "fail", "query",
             "last_row() 返回值异常, 疑并发写入时序问题"),
    TestCase("system-test", "python3 ./test.py -f 2-query/timetruncate.py -Q 4",
             "fail", "query",
             "WebSocket 模式(-Q 4)异常, -Q 1/2/3/R 均通过"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmq_connection.py",
             "fail", "tmq",
             "show connections 期望 2 实际 34, 测试隔离性问题"),
    TestCase("army", "python3 ./test.py -f vtable/test_vtable_query.py",
             "fail", "vtable",
             "浮点精度: RISC-V(-163781.01) vs x86 answer(-163685.41)"),
    TestCase("army", "python3 ./test.py -f vtable/test_vtable_query_cross_db.py",
             "fail", "vtable",
             "浮点精度: 跨库场景, 同 vtable_query 根因"),

    # ===================================================================
    # riscv-report.md: 4 TIMEOUT
    # ===================================================================
    TestCase("system-test", "python3 ./test.py -f 0-others/compatibility_rolling_upgrade.py -N 3",
             "timeout", "others",
             "3 节点滚动升级, RISC-V 启动约 x86 的 5 倍慢, ~15 分钟"),
    TestCase("army", "python3 ./test.py -f query/function/test_func_paramnum.py",
             "timeout", "query",
             "测试逻辑已 pass 但进程未退出, teardown 卡死"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/checkpoint_info.py -N 4",
             "timeout", "stream",
             "time.sleep(200) 硬编码 + 4 节点启动远超 480s"),
    TestCase("system-test", "python3 ./test.py -f 0-others/taosd_audit.py",
             "timeout", "others",
             "while True 死循环: threadisExit 在 RISC-V 上永不置位"),

    # ===================================================================
    # riscv-report.md: 1 NOT EXECUTED
    # ===================================================================
    TestCase("army", "python3 ./test.py -f storage/s3/s3Basic.py -N 3",
             "not_exec", "storage",
             "RISC-V 环境未配置 S3 存储后端"),

    # ===================================================================
    # riscvtest.sh 注释掉的: 69 DISABLED
    # ===================================================================

    # --- stream (21) ---
    TestCase("army",        "python3 ./test.py -f stream/test_stream_vtable.py",
             "disabled", "stream", "max retries exceeded"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/stream_multi_agg.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/stream_basic.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/scalar_function.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/at_once_interval.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/at_once_session.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/at_once_state_window.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/window_close_interval.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/window_close_session.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/window_close_state_window.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/max_delay_interval.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/max_delay_session.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/at_once_interval_ext.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/max_delay_interval_ext.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/window_close_session_ext.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/partition_interval.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/state_window_case.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/snode_restart_with_checkpoint.py -N 4",
             "disabled", "stream", "time limit exceeded"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/force_window_close_interp.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/force_window_close_interval.py",
             "disabled", "stream", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 8-stream/checkpoint_info2.py -N 4",
             "disabled", "stream", "no-fix retries exhausted"),

    # --- tmq (11) ---
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqDropStb.py",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqParamsTest.py -R",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqMaxGroupIds.py",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqDropConsumer.py",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqAutoCreateTbl.py",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqDelete-multiCtb.py -N 3 -n 3",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqUdf.py",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot0.py",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot1.py",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/tmq_taosx.py",
             "disabled", "tmq", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 7-tmq/walRemoveLog.py -N 3",
             "disabled", "tmq", "no-fix retries exhausted"),

    # --- query (13) ---
    TestCase("system-test", "python3 ./test.py -f 2-query/decimal3.py",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/decimal3.py -Q 4",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/decimal3.py -Q 3",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/decimal3.py -Q 2",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/decimal3.py -Q 1",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/insert_null_none.py -R",
             "disabled", "query", "time limit exceeded"),
    TestCase("system-test", "python3 ./test.py -f 2-query/agg_group_NotReturnValue.py -Q 3",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/large_data.py",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/char_length.py",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/char_length.py -R",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/char_length.py -Q 2",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/char_length.py -Q 3",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/char_length.py -Q 4",
             "disabled", "query", "no-fix retries exhausted"),

    # --- cols_function + window (5) ---
    TestCase("system-test", "python3 ./test.py -f 2-query/cols_function.py",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/cols_function.py -Q 2",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/cols_function.py -Q 3",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/cols_function.py -Q 4",
             "disabled", "query", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 2-query/test_window_true_for.py",
             "disabled", "query", "no-fix retries exhausted"),

    # --- insert (5) ---
    TestCase("system-test", "python3 ./test.py -f 1-insert/database_pre_suf.py",
             "disabled", "insert", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 1-insert/alter_database.py",
             "disabled", "insert", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 1-insert/rowlength64k_benchmark.py",
             "disabled", "insert", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 1-insert/precisionUS.py",
             "disabled", "insert", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 1-insert/precisionNS.py",
             "disabled", "insert", "no-fix retries exhausted"),

    # --- udf (5) ---
    TestCase("system-test", "python3 ./test.py -f 0-others/udfTest.py",
             "disabled", "udf", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/udf_create.py",
             "disabled", "udf", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/udf_restart_taosd.py",
             "disabled", "udf", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/udf_cfg1.py",
             "disabled", "udf", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/udf_cfg2.py",
             "disabled", "udf", "no-fix retries exhausted"),

    # --- others (7) ---
    TestCase("system-test", "python3 ./test.py -f 0-others/taosdlog.py",
             "disabled", "others", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/test_show_disk_usage.py",
             "disabled", "others", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/tag_index_basic.py",
             "disabled", "others", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/kill_balance_leader.py -N 3",
             "disabled", "others", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/dumpsdb.py",
             "disabled", "others", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/grant.py",
             "disabled", "others", "no-fix retries exhausted"),
    TestCase("system-test", "python3 ./test.py -f 0-others/compatibility.py",
             "disabled", "others", "no-fix retries exhausted"),

    # --- compatibility (1) ---
    TestCase("system-test", "python3 ./test.py -f 0-others/compatibility_rolling_upgrade_all.py -N 3",
             "disabled", "others", "no-fix retries exhausted"),

    # --- tools (1) ---
    TestCase("army", "python3 ./test.py -f tools/taosdump/ws/taosdumpRetry.py -B",
             "disabled", "tools", "max retries exceeded"),
]

assert len(CASES) == 82, f"Expected 82 cases, got {len(CASES)}"


# ─── Runner ──────────────────────────────────────────────────────────────────

TAG_ICON = {"fail": "❌", "timeout": "⏱️", "not_exec": "⚠️", "disabled": "#"}
RESULT_ICON = {"PASS": "✅", "FAIL": "❌", "TIMEOUT": "⏱️"}


def run_one(base_dir: Path, case: TestCase, timeout: int, log_file) -> CaseResult:
    start = datetime.now()
    ts = start.strftime("%H:%M:%S")
    print(f"  [{ts}] {case.subfolder}: {case.command}", flush=True)
    log_file.write(f"[START][{ts}] {case.subfolder}: {case.command}\n")
    log_file.flush()

    work_dir = base_dir / case.subfolder
    try:
        proc = subprocess.run(
            case.command, shell=True, cwd=str(work_dir),
            stdout=log_file, stderr=subprocess.STDOUT, timeout=timeout,
        )
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        rc = 124
    except Exception as e:
        log_file.write(f"Exception: {e}\n")
        rc = 1

    end = datetime.now()
    duration = (end - start).total_seconds()
    result = "PASS" if rc == 0 else ("TIMEOUT" if rc == 124 else "FAIL")

    icon = RESULT_ICON.get(result, "?")
    print(f"  {icon} [{result}] {duration:.0f}s  {case.command}", flush=True)
    log_file.write(f"[{result}][{end.strftime('%H:%M:%S')}] rc={rc} {duration:.0f}s\n\n")

    return CaseResult(
        subfolder=case.subfolder, command=case.command,
        tag=case.tag, module=case.module, result=result,
        rc=rc, duration_s=round(duration, 1),
        start_time=start.strftime("%Y-%m-%d %H:%M:%S"),
        end_time=end.strftime("%Y-%m-%d %H:%M:%S"),
    )


def print_summary(results: list[CaseResult], wall_start: datetime):
    total = len(results)
    passed = sum(1 for r in results if r.result == "PASS")
    failed = sum(1 for r in results if r.result == "FAIL")
    timed_out = sum(1 for r in results if r.result == "TIMEOUT")
    wall = (datetime.now() - wall_start).total_seconds()

    print(f"\n{'=' * 60}")
    print(f"  TOTAL: {total}  PASS: {passed}  FAIL: {failed}  TIMEOUT: {timed_out}")
    print(f"  Pass rate: {passed}/{total} = {passed/total*100:.1f}%")
    print(f"  Wall time: {wall:.0f}s ({wall/60:.1f}min)")
    print(f"{'=' * 60}")

    # Per-module breakdown
    modules = sorted(set(r.module for r in results))
    print(f"\n  {'Module':<12s} {'Total':>5s} {'Pass':>5s} {'Fail':>5s} {'T/O':>5s}")
    print(f"  {'-'*12} {'-'*5} {'-'*5} {'-'*5} {'-'*5}")
    for m in modules:
        mr = [r for r in results if r.module == m]
        p = sum(1 for r in mr if r.result == "PASS")
        f = sum(1 for r in mr if r.result == "FAIL")
        t = sum(1 for r in mr if r.result == "TIMEOUT")
        print(f"  {m:<12s} {len(mr):>5d} {p:>5d} {f:>5d} {t:>5d}")

    # List failures
    failures = [r for r in results if r.result != "PASS"]
    if failures:
        print(f"\n  Non-PASS details ({len(failures)}):")
        for r in failures:
            print(f"    [{r.result:7s}] [{r.tag:8s}] {r.subfolder}: {r.command}")
    print()


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="RISC-V 失败用例重测 (82 cases)")
    parser.add_argument("-t", "--timeout", type=int, default=480,
                        help="单例超时秒数 (default: 480)")
    parser.add_argument("-b", "--bin-dir", default=None,
                        help="taosd 二进制路径")
    parser.add_argument("-s", "--start-from", default=None,
                        help="从匹配的 case 开始")
    parser.add_argument("--tag", default=None,
                        choices=["fail", "timeout", "not_exec", "disabled"],
                        help="只跑指定 tag 的 case")
    parser.add_argument("--exclude-tag", default=None, nargs="+",
                        choices=["fail", "timeout", "not_exec", "disabled"],
                        help="排除指定 tag 的 case (可多个, 如 --exclude-tag fail timeout)")
    parser.add_argument("--module", default=None,
                        help="只跑指定模块 (stream/tmq/query/insert/udf/others/vtable/tools/storage)")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅预览, 不执行")
    parser.add_argument("--json", default=None, metavar="PATH",
                        help="输出 JSON 报告")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent

    # Activate venv if present and not already active
    venv_dir = base_dir / "venv"
    if venv_dir.is_dir() and "VIRTUAL_ENV" not in os.environ:
        activate = venv_dir / "bin" / "activate_this.py"
        # Use PATH-based activation (compatible with subprocess calls)
        os.environ["VIRTUAL_ENV"] = str(venv_dir)
        os.environ["PATH"] = str(venv_dir / "bin") + os.pathsep + os.environ.get("PATH", "")

    # Default TAOS_BIN_DIR — try release first, fall back to debug
    DEFAULT_BIN_DIRS = [
        "/media/eswin/sata/tsdb/release/build/bin",
        "/media/eswin/sata/tsdb/debug/build/bin",
    ]
    if args.bin_dir:
        p = Path(args.bin_dir)
    elif "TAOS_BIN_DIR" not in os.environ:
        p = None
        for d in DEFAULT_BIN_DIRS:
            if (Path(d) / "taosd").exists():
                p = Path(d)
                break
        if p is None:
            print(f"ERROR: taosd not found in any default path: {DEFAULT_BIN_DIRS}", file=sys.stderr)
            print("Use -b <bin_dir> or export TAOS_BIN_DIR", file=sys.stderr)
            sys.exit(1)
    else:
        p = None

    if p is not None:
        if not (p / "taosd").exists():
            print(f"ERROR: '{p}' 下未找到 taosd", file=sys.stderr)
            sys.exit(1)
        os.environ["TAOS_BIN_DIR"] = str(p.resolve())

    # Filter cases
    cases = list(CASES)
    if args.tag:
        cases = [c for c in cases if c.tag == args.tag]
    if args.exclude_tag:
        cases = [c for c in cases if c.tag not in args.exclude_tag]
    if args.module:
        cases = [c for c in cases if c.module == args.module]
    if args.start_from:
        skip = True
        filtered = []
        for c in cases:
            if skip and args.start_from in c.command:
                skip = False
            if not skip:
                filtered.append(c)
        cases = filtered

    # Dry-run
    if args.dry_run:
        # Summary by tag
        by_tag = {}
        for c in cases:
            by_tag.setdefault(c.tag, []).append(c)
        for tag in ["fail", "timeout", "not_exec", "disabled"]:
            group = by_tag.get(tag, [])
            if not group:
                continue
            icon = TAG_ICON[tag]
            print(f"\n{icon} {tag.upper()} ({len(group)}):")
            for c in group:
                print(f"  {c.subfolder:15s} {c.command}")
                print(f"    [{c.module}] {c.reason}")
        print(f"\n合计: {len(cases)} 个用例")
        return

    # Run
    wall_start = datetime.now()
    print(f"=== riscvtest_retry.py | {wall_start.strftime('%Y-%m-%d %H:%M:%S')} ===")
    print(f"用例数: {len(cases)}  超时: {args.timeout}s")
    print(f"Binary: {os.environ.get('TAOS_BIN_DIR', '(auto)')}\n")

    log_path = base_dir / "riscvtest_retry.log"
    results: list[CaseResult] = []

    with open(log_path, "a") as log_file:
        log_file.write(f"\n=== {wall_start.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        for i, case in enumerate(cases, 1):
            print(f"\n[{i}/{len(cases)}] {TAG_ICON[case.tag]} {case.tag} | {case.module}")
            r = run_one(base_dir, case, args.timeout, log_file)
            results.append(r)

    print_summary(results, wall_start)

    if args.json:
        report = {
            "start_time": wall_start.isoformat(),
            "end_time": datetime.now().isoformat(),
            "timeout": args.timeout,
            "total": len(results),
            "pass": sum(1 for r in results if r.result == "PASS"),
            "fail": sum(1 for r in results if r.result == "FAIL"),
            "timeout_count": sum(1 for r in results if r.result == "TIMEOUT"),
            "results": [asdict(r) for r in results],
        }
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"JSON: {args.json}")


if __name__ == "__main__":
    main()
