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

"""Copy the examples of an installer into a staging directory.

What ships inside an installer is an explicit list, kept in
packaging/examples-manifest.txt. The sources live under docs/examples with
normalized directory names (csharp, node), while the directory names inside a
package keep their historical spelling (C#, nodejs).

The shell packagers read the manifest directly. This script is for the release
scripts that cannot do that themselves (the Windows .bat and .py ones), so that
every installer ships the same files.

Usage:
  copy_examples.py --community <community checkout> --dst <examples dir>
                   [--group all]... [--jdbc] [--taosbenchmark-json] [--taosgen]
  copy_examples.py --community <community checkout> --dst <examples dir> --full
                   [--taosbenchmark-json] [--taosgen]

Use --full for the packages that historically shipped the whole docs/examples
tree (the enterprise tarball); the other installers ship the manifest subset.
Both modes skip build output (Maven target/, __pycache__, node_modules, ...).

A manifest entry whose source file is missing is an error: the release stops
instead of shipping a package with examples missing.
"""

import argparse
import os
import shutil
import sys

MANIFEST_REL_PATH = os.path.join("packaging", "examples-manifest.txt")
EXAMPLES_REL_PATH = os.path.join("docs", "examples")
JDBC_REL_PATH = os.path.join("docs", "examples", "JDBC")
TAOSBENCHMARK_REL_PATH = os.path.join("tools", "taosBenchmark", "example")
TAOSGEN_EXAMPLE_REL_PATH = os.path.join("docs", "examples", "taosgen")
# The taosgen configuration set lives outside the community checkout, next to it
# in the monorepo (source/taos-gen/conf). Callers whose layout differs can pass
# --taosgen-src explicitly.
TAOSGEN_CONF_REL_PATH = os.path.join("..", "taos-gen", "conf")

# Never ship build output: a JDBC example built in place leaves Maven target/
# (and .class files) behind, python examples leave __pycache__, node examples
# leave node_modules. makepkg.sh pruned target/ by hand before this script.
JUNK_PATTERNS = ("target", "__pycache__", "node_modules", ".git",
                 "*.pyc", "*.class", "*.o", "*.lo", "*.so")


def junk_ignore():
    return shutil.ignore_patterns(*JUNK_PATTERNS)


def read_manifest(path, groups):
    """Return the [(package dir, source path)] entries of the wanted groups."""
    entries = []
    with open(path, encoding="utf-8") as manifest:
        for number, line in enumerate(manifest, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            fields = line.split()
            if len(fields) != 3:
                raise SystemExit(
                    f"ERROR: {path}:{number}: expected "
                    f"'<group> <package dir> <source path>', got '{line}'"
                )
            group, pkg_dir, src_path = fields
            if group in groups:
                entries.append((pkg_dir, src_path))
    return entries


def copy_entry(community_dir, dst_dir, pkg_dir, src_path):
    """Copy one manifest entry into the package layout."""
    source = os.path.join(community_dir, EXAMPLES_REL_PATH, src_path)
    if not os.path.isfile(source):
        raise SystemExit(
            f"ERROR: example source missing: {source} "
            f"(see {os.path.join(community_dir, MANIFEST_REL_PATH)})"
        )
    # `csharp/connect/Program.cs` becomes `C#/connect/Program.cs`: the sub-path
    # is kept so that same-named files (Program.cs, main.go, ...) do not collide.
    relative = src_path.split("/", 1)[1] if "/" in src_path else src_path
    target = os.path.join(dst_dir, pkg_dir, relative)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    shutil.copyfile(source, target)


def copy_tree(source, target, ignore=None):
    if not os.path.isdir(source):
        raise SystemExit(f"ERROR: directory missing: {source}")
    shutil.copytree(source, target, dirs_exist_ok=True, ignore=ignore)


def main():
    parser = argparse.ArgumentParser(
        description="Copy installer examples from an explicit manifest"
    )
    parser.add_argument("--community", required=True,
                        help="community checkout, the one that holds docs/examples")
    parser.add_argument("--dst", required=True,
                        help="destination directory, the examples directory of the package")
    parser.add_argument("--group", action="append", default=None,
                        help="manifest group to ship (default: all, repeatable)")
    parser.add_argument("--full", action="store_true",
                        help="ship the whole docs/examples tree instead of the "
                             "manifest subset (historical enterprise tarball layout)")
    parser.add_argument("--jdbc", action="store_true",
                        help="also ship docs/examples/JDBC")
    parser.add_argument("--taosbenchmark-json", action="store_true",
                        help="also ship tools/taosBenchmark/example as taosbenchmark-json")
    parser.add_argument("--taosgen", action="store_true",
                        help="also ship docs/examples/taosgen as taosgen and the "
                             "taos-gen/conf configuration set as taosgen_config")
    parser.add_argument("--taosgen-src", default=None,
                        help="taos-gen/conf path, when it is not a sibling of "
                             "the community checkout (default: <community>/../taos-gen/conf)")
    args = parser.parse_args()

    community_dir = os.path.abspath(args.community)
    dst_dir = os.path.abspath(args.dst)
    groups = set(args.group or ["all"])

    os.makedirs(dst_dir, exist_ok=True)

    entries = []
    if args.full:
        # Same payload as the historical `cp -r docs/examples/*`: the shell
        # glob never matched dot entries, so `.gitignore` and friends stayed
        # out of the installer. Copy entry by entry to keep that behaviour.
        examples_dir = os.path.join(community_dir, EXAMPLES_REL_PATH)
        if not os.path.isdir(examples_dir):
            raise SystemExit(f"ERROR: directory missing: {examples_dir}")
        for entry in sorted(os.listdir(examples_dir)):
            if entry.startswith("."):
                continue
            source = os.path.join(examples_dir, entry)
            target = os.path.join(dst_dir, entry)
            if os.path.isdir(source):
                copy_tree(source, target, ignore=junk_ignore())
            else:
                shutil.copyfile(source, target)
    else:
        manifest = os.path.join(community_dir, MANIFEST_REL_PATH)
        if not os.path.isfile(manifest):
            raise SystemExit(f"ERROR: manifest missing: {manifest}")
        entries = read_manifest(manifest, groups)
        if not entries:
            raise SystemExit(
                f"ERROR: no {'/'.join(sorted(groups))} entries in {manifest}")
        for pkg_dir, src_path in entries:
            copy_entry(community_dir, dst_dir, pkg_dir, src_path)

    if args.jdbc and not args.full:
        copy_tree(os.path.join(community_dir, JDBC_REL_PATH),
                  os.path.join(dst_dir, "JDBC"), ignore=junk_ignore())
    if args.taosbenchmark_json:
        copy_tree(os.path.join(community_dir, TAOSBENCHMARK_REL_PATH),
                  os.path.join(dst_dir, "taosbenchmark-json"),
                  ignore=junk_ignore())
    if args.taosgen:
        copy_tree(os.path.join(community_dir, TAOSGEN_EXAMPLE_REL_PATH),
                  os.path.join(dst_dir, "taosgen"), ignore=junk_ignore())
        taosgen_conf = (os.path.abspath(args.taosgen_src) if args.taosgen_src
                        else os.path.join(community_dir, TAOSGEN_CONF_REL_PATH))
        if not os.path.isdir(taosgen_conf):
            raise SystemExit(
                f"ERROR: taos-gen configuration missing: {taosgen_conf} "
                f"(pass --taosgen-src when taos-gen is not next to the "
                f"community checkout)")
        copy_tree(taosgen_conf, os.path.join(dst_dir, "taosgen_config"),
                  ignore=junk_ignore())

    files = [os.path.join(root, name)
             for root, _, names in os.walk(dst_dir) for name in names]
    total_bytes = sum(os.path.getsize(f) for f in files)
    shipped = "full docs/examples tree" if args.full else \
        f"{len(entries)} manifest entries"
    print(f"examples: {shipped}, {len(files)} files, "
          f"{total_bytes} bytes copied into {dst_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
