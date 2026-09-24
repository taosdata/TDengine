#!/usr/bin/env python3
"""Run one self-contained taosQueryTest endurance round with durable evidence."""

import argparse
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


FAILURE_MARKERS = (
    "AddressSanitizer", "UndefinedBehaviorSanitizer", "runtime error:",
    "assertion failed", "Assertion `", "Segmentation fault", "core dumped",
    "taosd crash", "taosd crashed", "pair supervisor failed",
    "manager exited unexpectedly", "internal error",
)
SYSTEM_CORE_ROOTS = (Path("/var/lib/apport/coredump"), Path("/var/crash"))
RESULT_STORE_ENVIRONMENT = (
    "QST_RESULT_STORE_CONFIG_DIR",
    "QST_RESULT_STORE_ENDPOINT",
    "QST_RESULT_STORE_TAOSD_PID",
    "QST_RESULT_STORE_USER",
    "QST_RESULT_STORE_PASSWORD",
    "QST_RESULT_STORE_ALLOW_RESET",
)
RUNTIME_ENVIRONMENT = (
    "QST_TDENGINE_HOME",
    "QST_TAOSD",
    "QST_PROFILE_PHASES",
    "QST_PROFILE_STEADY",
    "QST_PROFILE_CATALOG",
)


def snapshot_cores(result_root):
    cores = set()
    if result_root.exists():
        for path in result_root.glob("*/cores/*"):
            if path.is_file():
                cores.add(str(path))
    for root in SYSTEM_CORE_ROOTS:
        if not root.is_dir():
            continue
        try:
            for path in root.iterdir():
                if path.is_file() and (root.name == "coredump" or
                                       "core" in path.name.lower()):
                    cores.add(str(path))
        except OSError:
            continue
    return cores


def result_directories(result_root):
    if not result_root.exists():
        return set()
    return {path.name for path in result_root.iterdir() if path.is_dir() and path.name.isdigit()}


def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def collect_product_issue_samples(log_text, new_cores):
    log_issues = [line for line in log_text.splitlines()
                  if any(marker.lower() in line.lower() for marker in FAILURE_MARKERS)]
    return log_issues + [f"new core file: {path}" for path in new_cores]


def build_execution_environment():
    environment = {
        "PATH": "/usr/bin:/bin",
        "HOME": os.environ.get("HOME", "/tmp"),
        "TZ": os.environ.get("TZ", "UTC"),
    }
    for name in RESULT_STORE_ENVIRONMENT:
        value = os.environ.get(name)
        if value:
            environment[name] = value
    for name in RUNTIME_ENVIRONMENT:
        value = os.environ.get(name)
        if value:
            environment[name] = value
    return environment


def prepare_execution_environment(cell):
    """Create the isolated runtime roots required by every audited subprocess."""
    runtime_tmp = cell / "runtime-tmp"
    runtime_home = runtime_tmp / "home"
    result_store_runtime = cell / "result-store-runtime"
    try:
        runtime_tmp.mkdir()
        runtime_home.mkdir()
        result_store_runtime.mkdir()
    except OSError as error:
        raise RuntimeError(
            f"cannot prepare audited runtime directories under {cell}: {error}") from error
    environment = build_execution_environment()
    environment["TMPDIR"] = str(runtime_tmp)
    environment["HOME"] = str(runtime_home)
    environment["QST_RUNTIME_TEMP_ROOT"] = str(runtime_tmp)
    environment["QST_PORT_LEASE_DIR"] = str(runtime_tmp / "port-leases")
    if not environment.get("QST_RESULT_STORE_CONFIG_DIR"):
        environment["QST_RESULT_STORE_RUNTIME_DIR"] = str(result_store_runtime)
    return environment


def cleanup_result_store_runtime(environment):
    """Release this round's private result-store files after its audit is saved."""
    runtime_dir = environment.get("QST_RESULT_STORE_RUNTIME_DIR")
    if not runtime_dir:
        return True
    try:
        shutil.rmtree(runtime_dir)
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return not Path(runtime_dir).exists()


def result_rows(entry):
    """Return typed query-result-v1 rows only when the audit entry is valid."""
    if not isinstance(entry, dict) or entry.get("ok") is not True:
        return None
    result = entry.get("result")
    if not isinstance(result, dict) or result.get("format") != "query-result-v1":
        return None
    rows = result.get("rows")
    if not isinstance(rows, list):
        return None
    return rows


def int_cell(cell):
    if not isinstance(cell, dict) or not isinstance(cell.get("value"), str):
        return None
    try:
        return int(cell["value"])
    except ValueError:
        return None


def one_count(entry):
    rows = result_rows(entry)
    if rows is None or len(rows) != 1 or not isinstance(rows[0], list) or len(rows[0]) != 1:
        return None
    return int_cell(rows[0][0])


def distribution_rows(entry, width):
    """Return fixed-width typed rows for an optional SQL distribution view."""
    rows = result_rows(entry)
    if rows is None:
        return []
    return [row for row in rows if isinstance(row, list) and len(row) == width]


def string_cell(cell):
    if not isinstance(cell, dict) or not isinstance(cell.get("value"), str):
        return None
    return cell["value"]


def analyze_sql_distribution(audit, providers):
    """Create non-threshold SQL diversity evidence from the durable audit."""
    analysis = {
        "classification": "overlapping_sql_shape_dimensions",
        "threshold_defined": False,
        "targets": {},
    }
    targets = audit.get("targets") if isinstance(audit, dict) else None
    if not isinstance(targets, dict):
        return analysis
    for target in providers.split(","):
        detail = targets.get(target)
        statement_count = one_count(detail.get("statement_count")) if isinstance(detail, dict) else None
        unique_count = one_count(detail.get("unique_sql_sha256_count")) if isinstance(detail, dict) else None
        shape_counts = {}
        fingerprint_top = []
        sql_samples = []
        if isinstance(detail, dict):
            shapes = detail.get("sql_shape")
            if isinstance(shapes, dict):
                for name, entry in shapes.items():
                    count = one_count(entry)
                    if count is not None:
                        shape_counts[name] = count
            for row in distribution_rows(detail.get("sql_fingerprint_top"), 2):
                fingerprint = string_cell(row[0])
                count = int_cell(row[1])
                if fingerprint is not None and count is not None:
                    fingerprint_top.append({"sql_sha256": fingerprint, "count": count})
            for row in distribution_rows(detail.get("sql_samples"), 2):
                fingerprint = string_cell(row[0])
                sql_text = string_cell(row[1])
                if fingerprint is not None and sql_text is not None:
                    sql_samples.append({"sql_sha256": fingerprint, "sql_text": sql_text})
        target_analysis = {
            "statement_count": statement_count,
            "unique_sql_sha256_count": unique_count,
            "unique_sql_ratio": (round(unique_count / statement_count, 6)
                                 if statement_count and unique_count is not None else None),
            "sql_shape_counts": shape_counts,
            "sql_shape_percent": {name: round(count * 100.0 / statement_count, 4)
                                  for name, count in shape_counts.items()}
                                if statement_count else {},
            "sql_fingerprint_top": fingerprint_top,
            "top_fingerprint_percent": (round(fingerprint_top[0]["count"] * 100.0 /
                                               statement_count, 4)
                                        if statement_count and fingerprint_top else None),
            "sql_samples": sql_samples,
        }
        analysis["targets"][target] = target_analysis
    return analysis


def audit_result_store(executable, run_id, providers, environment, cell):
    """Run package-local read-only audit and retain both protocol streams."""
    audit_path = cell / "result-store-audit.json"
    error_path = cell / "result-store-audit.err"
    command = [str(executable), "--audit-run", run_id, "--audit-targets", providers]
    with audit_path.open("w", encoding="utf-8") as stdout, \
         error_path.open("w", encoding="utf-8") as stderr:
        completed = subprocess.run(command, env=environment, stdout=stdout,
                                   stderr=stderr, check=False)
    audit = load_json(audit_path)
    return completed.returncode, audit


def reset_result_store(executable, run_id, environment, cell):
    """Drop the dedicated result database only after its audit is durable."""
    reset_path = cell / "result-store-reset.json"
    reset_error_path = cell / "result-store-reset.err"
    command = [str(executable), "--reset-result-store", run_id]
    with reset_path.open("w", encoding="utf-8") as stdout, \
            reset_error_path.open("w", encoding="utf-8") as stderr:
        completed = subprocess.run(command, env=environment, stdout=stdout,
                                   stderr=stderr, check=False)
    reset = load_json(reset_path)
    return completed.returncode, reset


def preflight_result_store(executable, environment, cell):
    """Verify native result-store connectivity before starting an endurance run."""
    audit_path = cell / "result-store-preflight.json"
    error_path = cell / "result-store-preflight.err"
    command = [str(executable), "--audit-run", "1", "--audit-targets", "td"]
    with audit_path.open("w", encoding="utf-8") as stdout, \
            error_path.open("w", encoding="utf-8") as stderr:
        completed = subprocess.run(command, env=environment, stdout=stdout,
                                   stderr=stderr, check=False)
    audit = load_json(audit_path)
    valid = isinstance(audit, dict) and audit.get("available") is True
    reasons = [] if valid else [
        "result-store preflight did not establish a native compatible connection"]
    return completed.returncode, audit, valid, reasons


def execution_status_counts(rows):
    """Return durable execution status counts when every row is well formed."""
    counts = {}
    if rows is None:
        return None
    for row in rows:
        if not isinstance(row, list) or len(row) != 2:
            return None
        status, count = (int_cell(cell) for cell in row)
        if status not in (0, 1, 2, 3) or count is None or count <= 0:
            return None
        counts[status] = counts.get(status, 0) + count
    return counts


def analyze_expected_execution(metadata_rows):
    """Compare execution outcomes with the generator's normal/abnormal class."""
    if metadata_rows is None:
        return None
    totals = {
        "statement_count": 0,
        "expected_outcome_count": 0,
        "normal_statement_count": 0,
        "normal_success_count": 0,
        "abnormal_statement_count": 0,
        "abnormal_error_count": 0,
    }
    for row in metadata_rows:
        if not isinstance(row, list) or len(row) != 5:
            return None
        _, abnormal, fallback, status, count = (int_cell(cell) for cell in row)
        if (abnormal not in (0, 1) or fallback not in (0, 1) or
                status not in (0, 1, 2, 3) or count is None or count <= 0):
            return None
        totals["statement_count"] += count
        expected_status = 1 if abnormal != 0 else 0
        if status == expected_status:
            totals["expected_outcome_count"] += count
        if abnormal == 0:
            totals["normal_statement_count"] += count
            if status == 0:
                totals["normal_success_count"] += count
        else:
            totals["abnormal_statement_count"] += count
            if status == 1:
                totals["abnormal_error_count"] += count
    statement_count = totals["statement_count"]
    totals["expected_outcome_ratio"] = (
        totals["expected_outcome_count"] / statement_count
        if statement_count else None)
    totals["normal_success_ratio"] = (
        totals["normal_success_count"] / totals["normal_statement_count"]
        if totals["normal_statement_count"] else None)
    totals["abnormal_error_ratio"] = (
        totals["abnormal_error_count"] / totals["abnormal_statement_count"]
        if totals["abnormal_statement_count"] else None)
    return totals


REQUIRED_RUNTIME_SHAPES = ("select", "from", "where", "aggregate")
EXTERNAL_TARGETS = frozenset(("mysql", "pg", "influx"))
DEFAULT_MINIMUM_QPS = 20000.0


def validate_result_store_audit(audit, run_id, providers, persisted, focus):
    """Validate durable ledgers without treating generated coverage errors as tool faults."""
    reasons = []
    target_counts = {}
    if not isinstance(audit, dict) or audit.get("complete") is not True:
        return False, ["audit document is missing or incomplete"], target_counts
    if audit.get("run_id") != run_id:
        return False, ["audit run_id does not match the runtime result directory"], target_counts
    if one_count(audit.get("run_record_count")) != 1:
        return False, ["result store does not contain exactly one run record"], target_counts
    targets = audit.get("targets")
    if not isinstance(targets, dict):
        return False, ["audit targets are missing"], target_counts
    total_statements = 0
    total_results = 0
    for target in providers.split(","):
        detail = targets.get(target)
        statement_count = one_count(detail.get("statement_count") if isinstance(detail, dict) else None)
        result_count = one_count(detail.get("result_count") if isinstance(detail, dict) else None)
        metadata_rows = result_rows(detail.get("generation_metadata") if isinstance(detail, dict) else None)
        execution_counts = execution_status_counts(
            result_rows(detail.get("execution_status") if isinstance(detail, dict) else None))
        expected_execution = analyze_expected_execution(metadata_rows)
        shape_counts = {}
        if isinstance(detail, dict) and isinstance(detail.get("sql_shape"), dict):
            shape_counts = {name: one_count(entry)
                            for name, entry in detail["sql_shape"].items()}
        source_reference_count = one_count(
            detail.get("source_reference_count") if isinstance(detail, dict) else None)
        target_counts[target] = {
            "statement_count": statement_count,
            "result_count": result_count,
            "execution_status_counts": execution_counts,
            "expected_execution": expected_execution,
            "sql_shape_counts": shape_counts,
            "source_reference_count": source_reference_count,
        }
        if statement_count is None or statement_count <= 0:
            reasons.append(f"{target}: statement count is missing or zero")
            continue
        if execution_counts is None or sum(execution_counts.values()) != statement_count:
            reasons.append(f"{target}: execution status does not cover all statements")
        if focus == "correctness" and (result_count is None or result_count != statement_count):
            reasons.append(f"{target}: correctness result count does not match statement count")
        for shape in REQUIRED_RUNTIME_SHAPES:
            if shape_counts.get(shape) is None or shape_counts[shape] <= 0:
                reasons.append(f"{target}: sql shape {shape} is missing or zero")
        if target in EXTERNAL_TARGETS:
            if shape_counts.get("external") is None or shape_counts["external"] <= 0:
                reasons.append(f"{target}: sql shape external is missing or zero")
            if source_reference_count is None or source_reference_count <= 0:
                reasons.append(f"{target}: provider source reference is missing or zero")
        if metadata_rows is None:
            reasons.append(f"{target}: generation metadata is unavailable")
            continue
        metadata_total = 0
        for row in metadata_rows:
            if not isinstance(row, list) or len(row) != 5:
                reasons.append(f"{target}: malformed generation metadata row")
                continue
            dialect, abnormal, fallback, status, count = (int_cell(cell) for cell in row)
            if (dialect not in (1, 2, 3, 4) or abnormal not in (0, 1) or
                    fallback not in (0, 1) or status not in (0, 1, 2, 3) or
                    count is None or count <= 0):
                reasons.append(f"{target}: invalid generation metadata value")
                continue
            metadata_total += count
        if metadata_total != statement_count:
            reasons.append(f"{target}: generation metadata does not cover all statements")
        target_counts[target]["generation_metadata_count"] = metadata_total
        if expected_execution is None or expected_execution["statement_count"] != statement_count:
            reasons.append(f"{target}: expected execution analysis is incomplete")
        else:
            target_counts[target]["expected_execution"] = expected_execution
            if focus == "correctness" and expected_execution["expected_outcome_count"] != statement_count:
                reasons.append(
                    f"{target}: execution outcomes disagree with normal/abnormal classification")
        total_statements += statement_count
        total_results += result_count or 0
    if persisted is None or persisted <= 0:
        reasons.append("report has no positive persisted task count")
    elif total_statements != persisted:
        reasons.append("result-store statement total does not match report persisted tasks")
    elif focus == "correctness" and total_results != persisted:
        reasons.append("result-store result total does not match report persisted tasks")
    return not reasons, reasons, target_counts


def validate_sustained_qps(profile, minimum_qps):
    """Validate active-window persisted QPS samples emitted by the runtime."""
    reasons = []
    throughput = profile.get("throughput") if isinstance(profile, dict) else None
    if not isinstance(throughput, dict):
        return False, ["stage profile has no sustained throughput evidence"], {}
    sample_count = throughput.get("persisted_qps_sample_count")
    minimum_sample_qps = throughput.get("minimum_persisted_qps")
    sample_window_ns = throughput.get("sample_window_ns")
    details = {
        "minimum_required_qps": minimum_qps,
        "warmup_window_ns": throughput.get("warmup_window_ns"),
        "sample_window_ns": sample_window_ns,
        "persisted_qps_sample_count": sample_count,
        "minimum_sample_qps": minimum_sample_qps,
    }
    if not isinstance(sample_count, int) or isinstance(sample_count, bool) or sample_count <= 0:
        reasons.append("stage profile has no active-window QPS samples")
    if not isinstance(sample_window_ns, int) or isinstance(sample_window_ns, bool) or sample_window_ns <= 0:
        reasons.append("stage profile has an invalid QPS sample window")
    if (not isinstance(minimum_sample_qps, (int, float)) or
            isinstance(minimum_sample_qps, bool) or
            not math.isfinite(minimum_sample_qps)):
        reasons.append("stage profile has no finite minimum QPS")
    elif minimum_sample_qps < minimum_qps:
        reasons.append(
            f"minimum sustained QPS {minimum_sample_qps:.2f} is below required {minimum_qps:.2f}")
    return not reasons, reasons, details


def make_runtime_package(package_root, cell):
    """Copy immutable package inputs so this round owns its result tree."""
    runtime_root = cell / "package"
    try:
        shutil.copytree(package_root, runtime_root,
                        ignore=shutil.ignore_patterns("results", "audited-rounds"))
        (runtime_root / "results").mkdir()
    except OSError as error:
        raise RuntimeError(f"cannot prepare isolated package runtime: {error}") from error
    return runtime_root


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("full", "random"), required=True)
    parser.add_argument("--providers", required=True)
    parser.add_argument("--round", type=int, required=True)
    parser.add_argument("--minutes", type=int, default=20)
    parser.add_argument("--minimum-qps", type=float, default=DEFAULT_MINIMUM_QPS)
    parser.add_argument("--focus", choices=("coverage", "crash", "correctness"),
                        default="coverage")
    parser.add_argument("--evidence-root", default=None)
    args = parser.parse_args()
    if args.round < 1:
        parser.error("--round must be positive")
    if args.minutes < 20:
        parser.error("--minutes must be at least 20")
    if not math.isfinite(args.minimum_qps) or args.minimum_qps <= 0:
        parser.error("--minimum-qps must be a positive finite number")
    if not re.fullmatch(r"td(?:,(?:mysql|pg|influx))*(?:,(?:mysql|pg|influx))*", args.providers):
        parser.error("--providers must be a comma-separated target set beginning with td")
    if len(args.providers.split(",")) != len(set(args.providers.split(","))):
        parser.error("--providers must not repeat targets")

    package_root = Path(__file__).resolve().parent
    executable = package_root / "taosQueryTest"
    if not executable.is_file() or not os.access(executable, os.X_OK):
        parser.error("taosQueryTest package executable is missing")
    evidence_root = (Path(args.evidence_root) if args.evidence_root else
                     package_root / "audited-rounds").resolve()
    cell = evidence_root / f"{args.mode}-{args.providers.replace(',', '_')}-r{args.round:02d}"
    if cell.exists():
        parser.error(f"refusing to overwrite existing evidence: {cell}")
    cell.mkdir(parents=True)

    try:
        environment = prepare_execution_environment(cell)
    except RuntimeError as error:
        parser.error(str(error))
    # Active-window QPS evidence is mandatory for every accepted endurance run.
    environment["QST_PROFILE_STEADY"] = "1"
    preflight_exit_code, preflight, preflight_valid, preflight_reasons = \
        preflight_result_store(executable, environment, cell)
    if not preflight_valid:
        validation = {
            "mode": args.mode,
            "providers": args.providers,
            "focus": args.focus,
            "round": args.round,
            "run_id": "",
            "elapsed_seconds": 0,
            "requested_minutes": args.minutes,
            "exit_code": preflight_exit_code,
            "result_store_preflight": preflight,
            "result_store_preflight_valid": False,
            "result_store_preflight_reasons": preflight_reasons,
            "passed": False,
        }
        (cell / "round-report.json").write_text(
            json.dumps(validation, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(validation, ensure_ascii=True, sort_keys=True))
        return 1

    try:
        runtime_root = make_runtime_package(package_root, cell)
    except RuntimeError as error:
        parser.error(str(error))
    executable = runtime_root / "taosQueryTest"
    result_root = runtime_root / "results"
    before_results = result_directories(result_root)
    before_cores = snapshot_cores(result_root)
    command = [str(executable), "-t", args.mode, "-f", args.focus, "-m",
               str(args.minutes), "-q", args.providers]
    started_wall = time.time()
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(started_wall))
    with (cell / "run.log").open("w", encoding="utf-8") as log:
        completed = subprocess.run(command, cwd=package_root, env=environment,
                                   stdout=log, stderr=subprocess.STDOUT,
                                   check=False)
    ended_wall = time.time()
    ended_at = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ended_wall))
    elapsed_seconds = ended_wall - started_wall
    created = result_directories(result_root) - before_results
    run_id = next(iter(created)) if len(created) == 1 else ""
    report = load_json(result_root / run_id / "report.json") if run_id else None
    stage_profile = load_json(result_root / run_id / "stage-profile.json") if run_id else None
    log_text = (cell / "run.log").read_text(encoding="utf-8", errors="replace")
    new_cores = sorted(snapshot_cores(result_root) - before_cores)
    product_issue_samples = collect_product_issue_samples(log_text, new_cores)
    tasks = report.get("tasks", {}) if report else {}
    processes = report.get("processes", {}) if report else {}
    task_counts_clear = all(tasks.get(name) == 0 for name in
                            ("executing", "final", "final_published", "persisting", "isolated"))
    process_accounted = (processes.get("registered") == processes.get("reaped") and
                         processes.get("running") == 0 and processes.get("failed") == 0)
    audit_exit_code = None
    audit = None
    audit_valid = False
    audit_reasons = ["runtime did not produce one result directory"]
    audit_target_counts = {}
    reset_exit_code = None
    reset = None
    reset_valid = True
    reset_reasons = ["result-store reset not requested"]
    sql_distribution = analyze_sql_distribution(None, args.providers)
    throughput_valid = False
    throughput_reasons = ["runtime did not produce a stage profile"]
    throughput = {
        "minimum_required_qps": args.minimum_qps,
        "sample_window_ns": None,
        "persisted_qps_sample_count": None,
        "minimum_sample_qps": None,
    }
    if run_id:
        audit_exit_code, audit = audit_result_store(executable, run_id, args.providers,
                                                    environment, cell)
        audit_valid, audit_reasons, audit_target_counts = validate_result_store_audit(
            audit, run_id, args.providers, tasks.get("persisted"), args.focus)
        sql_distribution = analyze_sql_distribution(audit, args.providers)
        throughput_valid, throughput_reasons, throughput = validate_sustained_qps(
            stage_profile, args.minimum_qps)
        if os.environ.get("QST_RESULT_STORE_ALLOW_RESET") == "1" and audit_valid:
            reset_exit_code, reset = reset_result_store(executable, run_id,
                                                        environment, cell)
            reset_valid = (reset_exit_code == 0 and isinstance(reset, dict) and
                           reset.get("complete") is True and
                           reset.get("run_id") == run_id and
                           reset.get("reset") is True)
            reset_reasons = [] if reset_valid else [
                "result-store reset did not complete after a valid audit"]
    result_store_runtime_removed = cleanup_result_store_runtime(environment)
    (cell / "sql-distribution.json").write_text(
        json.dumps(sql_distribution, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    passed = bool(
        completed.returncode == 0 and report and report.get("status") == "completed" and
        report.get("phases", {}).get("persistence") == "durable" and
        tasks.get("generated") == tasks.get("persisted") and task_counts_clear and
        process_accounted and audit_exit_code == 0 and audit_valid and
        reset_valid and result_store_runtime_removed and throughput_valid and
        not product_issue_samples and
        elapsed_seconds >= args.minutes * 60)
    validation = {
        "mode": args.mode,
        "providers": args.providers,
        "focus": args.focus,
        "round": args.round,
        "run_id": run_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "requested_minutes": args.minutes,
        "minimum_required_qps": args.minimum_qps,
        "exit_code": completed.returncode,
        "result_store_preflight": preflight,
        "result_store_preflight_exit_code": preflight_exit_code,
        "result_store_preflight_valid": preflight_valid,
        "result_store_preflight_reasons": preflight_reasons,
        "new_core_files": new_cores,
        "new_core_count": len(new_cores),
        "product_issue_count": len(product_issue_samples),
        "product_issue_samples": product_issue_samples[:20],
        "report": report,
        "result_store_audit_exit_code": audit_exit_code,
        "result_store_audit": audit,
        "result_store_audit_valid": audit_valid,
        "result_store_audit_reasons": audit_reasons,
        "result_store_target_counts": audit_target_counts,
        "result_store_reset_exit_code": reset_exit_code,
        "result_store_reset": reset,
        "result_store_reset_valid": reset_valid,
        "result_store_reset_reasons": reset_reasons,
        "result_store_runtime_removed": result_store_runtime_removed,
        "stage_profile": stage_profile,
        "sustained_qps_valid": throughput_valid,
        "sustained_qps_reasons": throughput_reasons,
        "sustained_qps": throughput,
        "sql_distribution_path": "sql-distribution.json",
        "generated_equals_persisted": bool(report and tasks.get("generated") == tasks.get("persisted")),
        "intermediate_tasks_cleared": task_counts_clear,
        "process_accounting_closed": process_accounted,
        "passed": passed,
    }
    (cell / "round-report.json").write_text(
        json.dumps(validation, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(validation, ensure_ascii=True, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
