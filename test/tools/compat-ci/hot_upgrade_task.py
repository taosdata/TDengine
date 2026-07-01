#!/usr/bin/env python3
"""Hot upgrade task for CI integration."""

import os
import sys
import re
import shutil
import subprocess
from pathlib import Path
from typing import Optional, List


class HotUpgradeTask:
    """Manages rolling upgrade test workflow."""

    def __init__(self):
        self.greenVersionsPath = os.getenv('TD_GREEN_VERSIONS_PATH', '/tdengine/green_versions/')
        self.currentVersion = None
        self.baseVersionPath = None
        self.targetVersionPath = None
        self.tempDir = None

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

    def findBaseVersions(self, targetVersion: str) -> List[str]:
        """Find matching base versions by first 3 digits."""
        parts = targetVersion.split('.')
        if len(parts) < 3:
            raise ValueError(f"Invalid version format: {targetVersion}")

        majorMinorPatch = f"{parts[0]}.{parts[1]}.{parts[2]}"

        if not os.path.exists(self.greenVersionsPath):
            return []

        matchedVersions = []
        for item in os.listdir(self.greenVersionsPath):
            itemPath = os.path.join(self.greenVersionsPath, item)
            if not os.path.isdir(itemPath):
                continue
            if item.startswith(majorMinorPatch):
                matchedVersions.append(item)

        matchedVersions.sort(key=lambda v: [int(x) for x in v.split('.')])
        return matchedVersions

    def prepareTargetVersion(self, buildDir: Optional[str] = None) -> str:
        """Copy target version files to /tmp/<version> directory."""
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
        libDst = Path(targetDir) / "libtaos.so"
        nativeSrc = buildPath / "build" / "lib" / "libtaosnative.so"
        taosSrc   = buildPath / "build" / "lib" / "libtaos.so"
        if nativeSrc.exists():
            shutil.copy2(nativeSrc, libDst)
            print(f"  Using libtaosnative.so -> libtaos.so")
        elif taosSrc.exists():
            shutil.copy2(taosSrc, libDst)
            print(f"  Using libtaos.so")
        else:
            raise FileNotFoundError(f"No libtaos.so or libtaosnative.so found in {buildPath / 'build' / 'lib'}")

        return targetDir

    def callCompatCheck(self, fromDir: str, toDir: str,
                        extraArgs: Optional[List[str]] = None) -> int:
        """Call compat-check tool for rolling upgrade."""
        compatCheckDir = Path(__file__).parent.parent / "compat-check"
        mainScript = compatCheckDir / "run" / "main.py"

        if not mainScript.exists():
            raise FileNotFoundError(f"compat-check script not found: {mainScript}")

        cmd = [
            sys.executable,
            str(mainScript),
            "-F", fromDir,
            "-T", toDir,
            "--rollupdate"
        ]

        if extraArgs:
            cmd.extend(extraArgs)

        print(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd)
        return result.returncode

    def run(self, buildDir: Optional[str] = None,
            extraArgs: Optional[List[str]] = None) -> int:
        """Execute rolling upgrade workflow."""
        try:
            # Print configuration
            print("=" * 80)
            print("Configuration:")
            print(f"  Green versions path: {self.greenVersionsPath}")
            print(f"  Build directory: {buildDir if buildDir else '../../../../debug (default)'}")
            if extraArgs:
                print(f"  Extra arguments: {' '.join(extraArgs)}")
            print("=" * 80)
            print()

            print("Step 1/7: Reading current version...")
            self.currentVersion = self.readCurrentVersion()
            print(f"Current version: {self.currentVersion}")

            print("\nStep 2/7: Finding matching base versions...")
            matchedVersions = self.findBaseVersions(self.currentVersion)

            if not matchedVersions:
                print(f"Info: No matching base version found (prefix: {'.'.join(self.currentVersion.split('.')[:3])})")
                print("No rolling upgrade test needed")
                return 0

            print(f"Found {len(matchedVersions)} version(s): {matchedVersions}")
            baseVersion = matchedVersions[0]
            self.baseVersionPath = os.path.join(self.greenVersionsPath, baseVersion)
            print(f"Selected base: {baseVersion}")
            print(f"Base path: {self.baseVersionPath}")

            print("\nStep 3/7: Preparing target version...")
            self.targetVersionPath = self.prepareTargetVersion(buildDir)
            print(f"Target path: {self.targetVersionPath}")

            print("\nStep 4/7: Running compat-check...")
            print("=" * 80)
            exitCode = self.callCompatCheck(
                self.baseVersionPath,
                self.targetVersionPath,
                extraArgs
            )
            print("=" * 80)

            if exitCode == 0:
                print("\n✓ Rolling upgrade test passed")
            else:
                print(f"\n✗ Test failed, exit code: {exitCode}")

            return exitCode

        except Exception as e:
            print(f"\n✗ Error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            return 1


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='TDengine Rolling Upgrade CI Task',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python hot_upgrade_task.py
  python hot_upgrade_task.py --build-dir /path/to/build
  python hot_upgrade_task.py -- --path /tmp/test --check-sysinfo
        """
    )

    parser.add_argument('--build-dir', help='Build directory (default: ../../../../debug)')
    parser.add_argument('--green-path', help='Green versions path (default: /tdengine/green_versions/)')

    args, extraArgs = parser.parse_known_args()

    task = HotUpgradeTask()

    if args.green_path:
        task.greenVersionsPath = args.green_path

    exitCode = task.run(buildDir=args.build_dir, extraArgs=extraArgs)
    sys.exit(exitCode)


if __name__ == '__main__':
    main()
