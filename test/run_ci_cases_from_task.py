#!/usr/bin/env python3

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Run all enabled local CI cases from test/ci/cases.task.

This script is a local sequential runner for the pytest-based CI task list.
It intentionally executes the active entries in the same order as they appear
in cases.task, while preserving each entry's working directory and command.

It does not try to reproduce GitLab's distributed scheduler. Instead, it
provides a deterministic host-side runner that can execute the entire active
cases.task locally and store logs under test/run/.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


@dataclass
class CaseEntry:
    index: int
    line_no: int
    priority: str
    rerun_times: str
    sanitizer: str
    workdir: str
    command: str
    raw_line: str


@dataclass
class CaseResult:
    index: int
    line_no: int
    sanitizer: str
    workdir: str
    command: str
    status: str
    exit_code: int
    elapsed_sec: float
    log_file: str


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    default_cases_task = script_dir / "ci" / "cases.task"
    default_log_dir = script_dir / "run" / f"local-ci-task-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    parser = argparse.ArgumentParser(
        description="Run all enabled cases from test/ci/cases.task locally."
    )
    parser.add_argument(
        "--cases-task",
        default=str(default_cases_task),
        help="Path to cases.task. Default: %(default)s",
    )
    parser.add_argument(
        "--log-dir",
        default=str(default_log_dir),
        help="Directory for per-case logs and summaries. Default: %(default)s",
    )
    parser.add_argument(
        "--stop-on-fail",
        action="store_true",
        help="Stop immediately after the first failed case.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the parsed cases without executing them.",
    )
    return parser.parse_args()


def _repo_root(test_dir: Path) -> Path:
    return test_dir.parents[2]


def build_base_env(test_dir: Path) -> dict[str, str]:
    repo_root = _repo_root(test_dir)
    env = os.environ.copy()

    taos_bin_path = env.get("TAOS_BIN_PATH")
    if not taos_bin_path:
        taos_bin_path = str(repo_root / "debug" / "build" / "bin")
        env["TAOS_BIN_PATH"] = taos_bin_path

    work_dir = env.get("WORK_DIR")
    if not work_dir:
        work_dir = str(repo_root / "sim")
        env["WORK_DIR"] = work_dir

    bin_path = Path(taos_bin_path)
    env["PATH"] = f"{bin_path}{os.pathsep}{env.get('PATH', '')}"

    lib_path = bin_path.parent / "lib"
    if lib_path.is_dir():
        current_ld = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"{lib_path}{os.pathsep}{current_ld}" if current_ld else str(lib_path)

    env["PYTHONUNBUFFERED"] = "1"
    return env


def parse_cases_task(cases_task: Path) -> list[CaseEntry]:
    cases: list[CaseEntry] = []
    with cases_task.open("r", encoding="utf-8") as fp:
        for line_no, raw_line in enumerate(fp, start=1):
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            parts = raw_line.rstrip("\n").split(",", 4)
            if len(parts) < 5:
                continue

            workdir = parts[3].strip()
            command = parts[4].strip()
            if not workdir or not command:
                continue

            cases.append(
                CaseEntry(
                    index=len(cases) + 1,
                    line_no=line_no,
                    priority=parts[0].strip(),
                    rerun_times=parts[1].strip(),
                    sanitizer=parts[2].strip() or "n",
                    workdir=workdir,
                    command=command,
                    raw_line=raw_line.rstrip("\n"),
                )
            )
    return cases


def sanitize_name(text: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", text.strip())
    return value[:120] if value else "case"


def normalize_case_command(command: str) -> str:
    """Add --clean to pytest-style commands unless the task already sets it."""
    try:
        argv = shlex.split(command, posix=True)
    except ValueError:
        return command

    if not argv or "--clean" in argv:
        return command

    if argv[0] == "pytest":
        return shlex.join([argv[0], "--clean", *argv[1:]])

    if len(argv) >= 2 and argv[0] == "./ci/pytest.sh" and argv[1] == "pytest":
        return shlex.join([argv[0], argv[1], "--clean", *argv[2:]])

    return command


def resolve_case_workdir(test_dir: Path, case: CaseEntry) -> Path:
    if case.workdir == ".":
        return test_dir
    return (test_dir / case.workdir).resolve()


def write_run_meta(log_dir: Path, cases_task: Path, env: dict[str, str], total_cases: int) -> None:
    meta = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "cases_task": str(cases_task),
        "total_cases": total_cases,
        "taos_bin_path": env.get("TAOS_BIN_PATH", ""),
        "work_dir": env.get("WORK_DIR", ""),
        "cwd": str(Path.cwd()),
    }
    (log_dir / "run-meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def run_case(case: CaseEntry, test_dir: Path, env: dict[str, str], log_dir: Path) -> CaseResult:
    case_workdir = resolve_case_workdir(test_dir, case)
    effective_command = normalize_case_command(case.command)
    log_name = f"{case.index:04d}-{sanitize_name(case.command)}.log"
    log_path = log_dir / log_name
    started = time.monotonic()

    if not case_workdir.is_dir():
        log_path.write_text(
            f"[runner] ERROR: workdir does not exist: {case_workdir}\n"
            f"[runner] raw line: {case.raw_line}\n",
            encoding="utf-8",
        )
        return CaseResult(
            index=case.index,
            line_no=case.line_no,
            sanitizer=case.sanitizer,
            workdir=str(case_workdir),
            command=case.command,
            status="FAIL",
            exit_code=1,
            elapsed_sec=0.0,
            log_file=str(log_path),
        )

    with log_path.open("w", encoding="utf-8") as log_fp:
        log_fp.write(f"[runner] case_index={case.index}\n")
        log_fp.write(f"[runner] line_no={case.line_no}\n")
        log_fp.write(f"[runner] sanitizer={case.sanitizer}\n")
        log_fp.write(f"[runner] workdir={case_workdir}\n")
        log_fp.write(f"[runner] command={case.command}\n")
        log_fp.write(f"[runner] effective_command={effective_command}\n")
        log_fp.write(f"[runner] started_at={datetime.now().isoformat(timespec='seconds')}\n")
        log_fp.write("\n")
        log_fp.flush()

        proc = subprocess.Popen(
            ["bash", "-lc", effective_command],
            cwd=str(case_workdir),
            env=env,
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        try:
            exit_code = proc.wait()
            status = "PASS" if exit_code == 0 else "FAIL"
        except KeyboardInterrupt:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
            raise

    elapsed = time.monotonic() - started
    return CaseResult(
        index=case.index,
        line_no=case.line_no,
        sanitizer=case.sanitizer,
        workdir=str(case_workdir),
        command=case.command,
        status=status,
        exit_code=exit_code,
        elapsed_sec=elapsed,
        log_file=str(log_path),
    )


def print_case(case: CaseEntry) -> None:
    effective_command = normalize_case_command(case.command)
    print(
        f"[{case.index:04d}] line={case.line_no} san={case.sanitizer} "
        f"workdir={case.workdir} cmd={effective_command}"
    )


def write_summary(log_dir: Path, results: Iterable[CaseResult]) -> None:
    results = list(results)
    summary_json = [asdict(result) for result in results]
    (log_dir / "summary.json").write_text(
        json.dumps(summary_json, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    lines = ["index\tline_no\tstatus\texit_code\telapsed_sec\tsanitizer\tworkdir\tcommand\tlog_file"]
    for result in results:
        lines.append(
            "\t".join(
                [
                    str(result.index),
                    str(result.line_no),
                    result.status,
                    str(result.exit_code),
                    f"{result.elapsed_sec:.3f}",
                    result.sanitizer,
                    result.workdir,
                    result.command,
                    result.log_file,
                ]
            )
        )
    (log_dir / "summary.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    test_dir = Path(__file__).resolve().parent
    cases_task = Path(args.cases_task).resolve()
    log_dir = Path(args.log_dir).resolve()

    if not cases_task.is_file():
        print(f"ERROR: cases.task not found: {cases_task}", file=sys.stderr)
        return 1

    cases = parse_cases_task(cases_task)
    if not cases:
        print(f"ERROR: no active cases found in {cases_task}", file=sys.stderr)
        return 1

    log_dir.mkdir(parents=True, exist_ok=True)
    env = build_base_env(test_dir)
    write_run_meta(log_dir, cases_task, env, len(cases))

    print(f"[runner] test_dir      : {test_dir}")
    print(f"[runner] cases_task    : {cases_task}")
    print(f"[runner] log_dir       : {log_dir}")
    print(f"[runner] TAOS_BIN_PATH : {env.get('TAOS_BIN_PATH', '')}")
    print(f"[runner] WORK_DIR      : {env.get('WORK_DIR', '')}")
    print(f"[runner] total_cases   : {len(cases)}")

    if args.dry_run:
        for case in cases:
            print_case(case)
        print("[runner] dry-run only; no case was executed")
        return 0

    results: list[CaseResult] = []
    failures = 0

    for case in cases:
        print_case(case)
        result = run_case(case, test_dir, env, log_dir)
        results.append(result)
        print(
            f"[runner] result index={result.index:04d} status={result.status} "
            f"rc={result.exit_code} elapsed={result.elapsed_sec:.3f}s log={result.log_file}"
        )
        if result.status != "PASS":
            failures += 1
            if args.stop_on_fail:
                print("[runner] stop-on-fail enabled; aborting after first failure")
                break

    write_summary(log_dir, results)

    total = len(results)
    passed = sum(1 for result in results if result.status == "PASS")
    failed = total - passed
    print(
        f"[runner] summary total={total} pass={passed} fail={failed} "
        f"summary={log_dir / 'summary.tsv'}"
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
