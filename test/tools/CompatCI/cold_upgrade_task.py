#!/usr/bin/env python3
"""Cold upgrade task for CI integration."""

import os
import sys
import re
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import List, Tuple, Optional


class ColdUpgradeTask:
    """Manages cold upgrade test workflow."""

    def __init__(self):
        self.greenVersionsPath = os.getenv('TD_GREEN_VERSIONS_PATH', '/tdengine/green_versions/')
        self.baseVersions: List[str] = []
        self.currentVersion: Optional[str] = None
        self.targetVersionPath: Optional[str] = None
        self.tempDir: Optional[str] = None

    # ------------------------------------------------------------------
    # Argument parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def parseBaseVersions(raw: str) -> List[str]:
        """Parse 'VER,VER,...' into a list of base version strings.

        Example:
            '3.3.0.0,3.3.6.0'
            -> ['3.3.0.0', '3.3.6.0']
        """
        versionRe = re.compile(r'^\d+\.\d+\.\d+\.\d+$')
        versions: List[str] = []

        for token in raw.split(','):
            token = token.strip()
            if not token:
                continue
            if not versionRe.match(token):
                raise ValueError(
                    f"Invalid version number '{token}': expected x.x.x.x format"
                )
            versions.append(token)

        if not versions:
            raise ValueError("No valid base versions provided")

        return versions

    # ------------------------------------------------------------------
    # Current version resolution (from cmake)
    # ------------------------------------------------------------------

    def readCurrentVersion(self) -> str:
        """Read version from cmake/version.cmake."""
        cmakeFile = Path(__file__).parent.parent.parent.parent / "cmake" / "version.cmake"

        if not cmakeFile.exists():
            raise FileNotFoundError(f"Version file not found: {cmakeFile}")

        with open(cmakeFile, 'r') as f:
            content = f.read()

        match = re.search(r'SET\s*\(\s*TD_VER_NUMBER\s+"([^"]+)"\s*\)', content)
        if not match:
            raise ValueError(f"Cannot parse version from {cmakeFile}")

        version = match.group(1)
        version = re.match(r'(\d+\.\d+\.\d+\.\d+)', version).group(1)
        return version

    # ------------------------------------------------------------------
    # Target version preparation (from build directory)
    # ------------------------------------------------------------------

    def prepareTargetVersion(self, buildDir: Optional[str] = None) -> str:
        """Copy target version files from build directory to /tmp/<version>."""
        targetDir = f"/tmp/{self.currentVersion}"
        os.makedirs(targetDir, exist_ok=True)
        self.tempDir = targetDir

        if not buildDir:
            buildDir = Path(__file__).parent.parent.parent.parent.parent / "debug"

        buildPath = Path(buildDir)
        if not buildPath.exists():
            raise FileNotFoundError(f"Build directory not found: {buildDir}")

        # Copy taosd binary
        taosdSrc = buildPath / "build" / "bin" / "taosd"
        if taosdSrc.exists():
            shutil.copy2(taosdSrc, targetDir)

        # Prefer libtaosnative.so (real native lib) renamed to libtaos.so,
        # fall back to libtaos.so if libtaosnative.so is absent
        libDst    = Path(targetDir) / "libtaos.so"
        nativeSrc = buildPath / "build" / "lib" / "libtaosnative.so"
        taosSrc   = buildPath / "build" / "lib" / "libtaos.so"
        if nativeSrc.exists():
            shutil.copy2(nativeSrc, libDst)
            print(f"  Using libtaosnative.so -> libtaos.so")
        elif taosSrc.exists():
            shutil.copy2(taosSrc, libDst)
            print(f"  Using libtaos.so")
        else:
            raise FileNotFoundError(
                f"No libtaos.so or libtaosnative.so found in {buildPath / 'build' / 'lib'}"
            )

        return targetDir

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
        """Resolve every base version to (fromDir, toDir).

        fromDir comes from greenVersionsPath/<baseVersion>.
        toDir   is the pre-built target version in /tmp/<currentVersion>.

        Checks all base versions before raising so the user gets a complete
        picture of any missing directories in one run.
        """
        errors: List[str] = []
        resolved: List[Tuple[str, str]] = []

        for baseVer in self.baseVersions:
            try:
                fromDir = self.resolveVersionDir(baseVer)
                resolved.append((fromDir, self.targetVersionPath))
            except FileNotFoundError as exc:
                errors.append(str(exc))

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

    def run(self, buildDir: Optional[str] = None,
            extraArgs: Optional[List[str]] = None) -> int:
        """Execute cold upgrade workflow for all requested base versions."""
        try:
            # ── Print configuration ──────────────────────────────────
            print("=" * 80)
            print("Cold Upgrade Configuration:")
            print(f"  Green versions path : {self.greenVersionsPath}")
            print(f"  Build directory     : {buildDir if buildDir else '../../../../debug (default)'}")
            print(f"  Base versions       : {', '.join(self.baseVersions)}")
            if extraArgs:
                print(f"  CompatCheck options : {' '.join(extraArgs)}")
            print("=" * 80)
            print()

            # ── Step 1: Read current version from cmake ──────────────
            print("Step 1/3: Reading current version from cmake...")
            self.currentVersion = self.readCurrentVersion()
            print(f"  Current (target) version: {self.currentVersion}")
            print()

            # ── Step 2: Prepare target version from build directory ──
            print("Step 2/3: Preparing target version from build directory...")
            self.targetVersionPath = self.prepareTargetVersion(buildDir)
            print(f"  Target path: {self.targetVersionPath}")
            print()

            # ── Step 3: Resolve base dirs and run CompatCheck ────────
            print(f"Step 3/3: Resolving base version directories under "
                  f"'{self.greenVersionsPath}'...")
            resolvedPairs = self.resolveAllPairs()
            print(f"  All {len(resolvedPairs)} base version(s) resolved successfully.")
            for fromDir, toDir in resolvedPairs:
                print(f"  FROM : {fromDir}")
                print(f"  TO   : {toDir}")
            print()

            print("Running cold upgrade tests...")
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
  # Single base version  (target is read from build directory automatically)
  python cold_upgrade_task.py \\
      --green-path /tdengine/green_versions/ \\
      --versions 3.3.0.0

  # Multiple base versions (comma-separated)
  python cold_upgrade_task.py \\
      --green-path /tdengine/green_versions/ \\
      --versions 3.3.0.0,3.3.6.0

  # Custom build directory
  python cold_upgrade_task.py \\
      --green-path /tdengine/green_versions/ \\
      --versions 3.3.0.0 \\
      --build-dir /path/to/debug

  # Pass extra options to CompatCheck
  python cold_upgrade_task.py \\
      --green-path /tdengine/green_versions/ \\
      --versions 3.3.0.0 \\
      --options "-q --check-sysinfo"
        """
    )

    parser.add_argument(
        '--green-path',
        help='Green versions storage path (default: /tdengine/green_versions/ '
             'or env TD_GREEN_VERSIONS_PATH)',
    )
    parser.add_argument(
        '--versions',
        required=True,
        metavar='VER[,VER,...]',
        help=(
            'Comma-separated list of base versions to upgrade FROM, '
            'e.g. 3.3.0.0,3.3.6.0  '
            '(the target version is detected automatically from the build directory)'
        ),
    )
    parser.add_argument(
        '--build-dir',
        help='Build directory containing compiled binaries (default: ../../../../debug)',
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
        task.baseVersions = ColdUpgradeTask.parseBaseVersions(args.versions)
    except ValueError as exc:
        print(f"[ERROR] Argument error: {exc}", file=sys.stderr)
        sys.exit(1)

    # Split --options string into a proper argument list
    extraArgs = shlex.split(args.options) if args.options.strip() else []

    sys.exit(task.run(buildDir=args.build_dir, extraArgs=extraArgs))


if __name__ == '__main__':
    main()
