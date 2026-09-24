#!/usr/bin/env python3
"""Run or resume the self-contained taosQueryTest endurance matrix."""

import argparse
import json
import math
from pathlib import Path
import subprocess
import sys


TARGETS = (
    "td",
    "td,mysql",
    "td,pg",
    "td,influx",
    "td,mysql,pg",
    "td,mysql,influx",
    "td,pg,influx",
    "td,mysql,pg,influx",
)


def prior_round_passed(path, minimum_qps):
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    sustained = payload.get("sustained_qps")
    return bool(
        payload.get("passed") and
        payload.get("elapsed_seconds", 0) >= payload.get("requested_minutes", 0) * 60 and
        payload.get("minimum_required_qps") == minimum_qps and
        payload.get("sustained_qps_valid") is True and
        isinstance(sustained, dict) and
        sustained.get("minimum_sample_qps", 0) >= minimum_qps)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--minimum-qps", type=float, default=20000.0)
    parser.add_argument("--evidence-root", default=None)
    parser.add_argument("--modes", default="full,random")
    parser.add_argument("--targets", default=";".join(TARGETS))
    parser.add_argument("--focus", choices=("coverage", "crash", "correctness"),
                        default="coverage")
    args = parser.parse_args()
    if args.minutes < 20:
        parser.error("--minutes must be at least 20")
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    if not math.isfinite(args.minimum_qps) or args.minimum_qps <= 0:
        parser.error("--minimum-qps must be a positive finite number")
    modes = tuple(args.modes.split(","))
    if not modes or any(mode not in ("full", "random") for mode in modes):
        parser.error("--modes must contain full and/or random")
    targets = tuple(args.targets.split(";"))
    if not targets or any(not target for target in targets):
        parser.error("--targets must be semicolon-separated target sets")

    package_root = Path(__file__).resolve().parent
    runner = package_root / "run-audited-round.py"
    if not runner.is_file():
        parser.error("run-audited-round.py is missing from the package")
    evidence_root = (Path(args.evidence_root) if args.evidence_root else
                     package_root / "audited-rounds").resolve()
    evidence_root.mkdir(parents=True, exist_ok=True)
    for mode in modes:
        for target in targets:
            cell_prefix = f"{mode}-{target.replace(',', '_')}-r"
            for round_number in range(1, args.rounds + 1):
                report = evidence_root / f"{cell_prefix}{round_number:02d}" / "round-report.json"
                if report.exists():
                    if prior_round_passed(report, args.minimum_qps):
                        continue
                    print(f"matrix stopped: existing failed or incomplete evidence: {report}",
                          file=sys.stderr)
                    return 1
                command = [str(runner), "--mode", mode, "--providers", target,
                           "--round", str(round_number), "--minutes", str(args.minutes),
                           "--focus", args.focus,
                           "--minimum-qps", str(args.minimum_qps),
                           "--evidence-root", str(evidence_root)]
                completed = subprocess.run(command, cwd=package_root, check=False)
                if completed.returncode != 0:
                    print(f"matrix stopped: {mode} {target} round {round_number} failed",
                          file=sys.stderr)
                    return completed.returncode or 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
