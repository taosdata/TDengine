#!/usr/bin/env python3
"""Cold upgrade task for CI integration."""

import os
import sys
import re
import shlex
import subprocess
from pathlib import Path
from typing import List, Tuple, Optional


class ColdUpgradeTask:
    """Manages cold upgrade test workflow."""

    def __init__(self):
        self.greenVersionsPath = os.getenv('TD_GREEN_VERSIONS_PATH', '/tdengine/green_versions/')
        self.versionPairs: List[Tuple[str, str]] = []

    # ------------------------------------------------------------------
    # Argument parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def parseVersionPairs(raw: str) -> List[Tuple[str, str]]:
        """Parse 'FROM->TO,FROM->TO,...' into a list of (from, to) tuples.

        Example:
            '3.3.0.0->3.3.6.0,3.3.6.0->3.4.0.0'
            -> [('3.3.0.0', '3.3.6.0'), ('3.3.6.0', '3.4.0.0')]
        """
        versionRe = re.compile(r'^\d+\.\d+\.\d+\.\d+$')
        pairs: List[Tuple[str, str]] = []

        for token in raw.split(','):
            token = token.strip()
            if not token:
                continue
            parts = token.split('->')
            if len(parts) != 2:
                raise ValueError(
                    f"Invalid task '{token}': expected format FROM->TO "
                    f"(e.g. 3.3.0.0->3.3.6.0)"
                )
            fromVer, toVer = parts[0].strip(), parts[1].strip()
            for ver in (fromVer, toVer):
                if not versionRe.match(ver):
                    raise ValueError(
                        f"Invalid version number '{ver}': expected x.x.x.x format"
                    )
            pairs.append((fromVer, toVer))

        if not pairs:
            raise ValueError("No valid tasks provided")

        return pairs

    # ------------------------------------------------------------------
    # Path resolution
    # ------------------------------------------------------------------

    def resolveVersionDir(self, version: str) -> str:
        """Return the absolute path for *version* under greenVersionsPath.

        Raises FileNotFoundError if the directory does not exist.
        """
        dirPath = os.path.join(self.greenVersionsPath, version)
        if not os.path.isdir(dirPath):
            raise FileNotFoundError(
                f"Version directory not found for '{version}': {dirPath}"
            )
        return dirPath

    def resolveAllPairs(self) -> List[Tuple[str, str]]:
        """Resolve every (fromVer, toVer) pair to (fromDir, toDir).

        Checks all pairs before raising so the user gets a complete
        picture of any missing directories in one run.
        """
        errors: List[str] = []
        resolved: List[Tuple[str, str]] = []

        for fromVer, toVer in self.versionPairs:
            fromDir = toDir = None
            try:
                fromDir = self.resolveVersionDir(fromVer)
            except FileNotFoundError as exc:
                errors.append(str(exc))

            try:
                toDir = self.resolveVersionDir(toVer)
            except FileNotFoundError as exc:
                errors.append(str(exc))

            if fromDir and toDir:
                resolved.append((fromDir, toDir))

        if errors:
            raise FileNotFoundError(
                "One or more version directories are missing:\n"
                + "\n".join(f"  * {e}" for e in errors)
            )

        return resolved

    # ------------------------------------------------------------------
    # CompatCheck invocation
    # ------------------------------------------------------------------

    def callCompatCheck(self, fromDir: str, toDir: str,
                        extraArgs: Optional[List[str]] = None) -> int:
        """Call CompatCheck tool for cold upgrade (default mode, no extra flag needed).

        Cold upgrade is the default behaviour of CompatCheck/run/main.py.
        Hot/rolling upgrade requires the explicit -r / --rollupdate flag.
        """
        compatCheckDir = Path(__file__).parent.parent / "CompatCheck"
        mainScript = compatCheckDir / "run" / "main.py"

        if not mainScript.exists():
            raise FileNotFoundError(
                f"CompatCheck script not found: {mainScript}"
            )

        cmd = [
            sys.executable,
            str(mainScript),
            "-F", fromDir,
            "-T", toDir,
            # No extra flag: cold upgrade is the default mode
        ]

        if extraArgs:
            cmd.extend(extraArgs)

        print(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd)
        return result.returncode

    # ------------------------------------------------------------------
    # Main workflow
    # ------------------------------------------------------------------

    def run(self, extraArgs: Optional[List[str]] = None) -> int:
        """Execute cold upgrade workflow for all requested version pairs."""
        try:
            # ── Print configuration ──────────────────────────────────
            tasks = [f"{f}->{t}" for f, t in self.versionPairs]
            print("=" * 80)
            print("Cold Upgrade Configuration:")
            print(f"  Green versions path : {self.greenVersionsPath}")
            print(f"  Tasks               : {', '.join(tasks)}")
            if extraArgs:
                print(f"  CompatCheck options : {' '.join(extraArgs)}")
            print("=" * 80)
            print()

            # ── Step 1: Resolve all version directories ──────────────
            print(f"Step 1/2: Resolving version directories under "
                  f"'{self.greenVersionsPath}'...")
            resolvedPairs = self.resolveAllPairs()
            print(f"  All {len(resolvedPairs)} task(s) resolved successfully.")
            for fromDir, toDir in resolvedPairs:
                print(f"  FROM : {fromDir}")
                print(f"  TO   : {toDir}")
            print()

            # ── Step 2: Run CompatCheck for each pair ────────────────
            print("Step 2/2: Running cold upgrade tests...")
            overallCode = 0

            for idx, (fromDir, toDir) in enumerate(resolvedPairs, start=1):
                fromVer = os.path.basename(fromDir)
                toVer   = os.path.basename(toDir)
                print()
                print(f"[{idx}/{len(resolvedPairs)}] {fromVer}  ->  {toVer}")
                print("=" * 80)

                exitCode = self.callCompatCheck(fromDir, toDir, extraArgs)

                print("=" * 80)
                if exitCode == 0:
                    print(f"[PASS] Cold upgrade test passed  ({fromVer} -> {toVer})")
                else:
                    print(f"[FAIL] Cold upgrade test FAILED  ({fromVer} -> {toVer}), "
                          f"exit code: {exitCode}")
                    overallCode = exitCode

            print()
            if overallCode == 0:
                print("[PASS] All cold upgrade tests passed.")
            else:
                print(f"[FAIL] One or more cold upgrade tests failed "
                      f"(last non-zero exit code: {overallCode}).")

            return overallCode

        except Exception as exc:
            print(f"\n[ERROR] {exc}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            return 1


# ──────────────────────────────────────────────────────────────────────────────

def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='TDengine Cold Upgrade CI Task',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single task
  python cold_upgrade_task.py \\
      --green-path /tdengine/green_versions/ \\
      --tasks 3.3.0.0->3.3.6.0

  # Multiple tasks (comma-separated)
  python cold_upgrade_task.py \\
      --green-path /tdengine/green_versions/ \\
      --tasks 3.3.0.0->3.3.6.0,3.3.6.0->3.4.0.0

  # Pass extra options to CompatCheck (quick mode + sysinfo check)
  python cold_upgrade_task.py \\
      --green-path /tdengine/green_versions/ \\
      --tasks 3.3.0.0->3.4.0.0 \\
      --options "-q --check-sysinfo"
        """
    )

    parser.add_argument(
        '--green-path',
        help='Green versions storage path (default: /tdengine/green_versions/ '
             'or env TD_GREEN_VERSIONS_PATH)',
    )
    parser.add_argument(
        '--tasks',
        required=True,
        metavar='FROM->TO[,FROM->TO,...]',
        help=(
            'Comma-separated list of FROM->TO upgrade tasks, '
            'e.g. 3.3.0.0->3.3.6.0,3.3.6.0->3.4.0.0'
        ),
    )
    parser.add_argument(
        '--options',
        metavar='COMPAT_CHECK_OPTIONS',
        default='',
        help=(
            'Extra options passed directly to CompatCheck, '
            'e.g. --options "-q --check-sysinfo"'
        ),
    )

    args = parser.parse_args()

    task = ColdUpgradeTask()

    if args.green_path:
        task.greenVersionsPath = args.green_path

    try:
        task.versionPairs = ColdUpgradeTask.parseVersionPairs(args.tasks)
    except ValueError as exc:
        print(f"[ERROR] Argument error: {exc}", file=sys.stderr)
        sys.exit(1)

    # Split --options string into a proper argument list
    extraArgs = shlex.split(args.options) if args.options.strip() else []

    sys.exit(task.run(extraArgs=extraArgs))


if __name__ == '__main__':
    main()
