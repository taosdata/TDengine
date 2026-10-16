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

"""Stable lifecycle command for TDgpt Taosanode resources and workers."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

COMMANDS = {
    ("service", "start"): "start",
    ("service", "stop"): "stop",
    ("service", "status"): "status",
    ("service", "wait-ready"): "wait-ready",
    ("model", "start"): "model-start",
    ("model", "stop"): "model-stop",
    ("model", "status"): "model-status",
    ("resource", "import"): "resource-import",
    ("resource", "check"): "resource-check",
    ("resource", "apply"): "resource-apply",
    ("resource", "list"): "resource-list",
    ("resource", "verify"): "resource-verify",
    ("resource", "delete"): "resource-delete",
}

VERSION_BANNER = "TDgpt - TDengine TSDB(c) Time-Series Data Analytics Platform (ver {version})"


def get_version_banner(script_path: Path | None = None) -> str:
    """Read the packaged TDgpt version without importing runtime dependencies."""
    script_path = (script_path or Path(__file__)).resolve()
    install_root = script_path.parent.parent
    candidates = (
        install_root / "lib" / "taosanalytics" / "__init__.py",
        install_root / "taosanalytics" / "__init__.py",
    )
    for version_file in candidates:
        if not version_file.is_file():
            continue
        match = re.search(
            r'^__version__\s*=\s*["\']([^"\']+)["\']',
            version_file.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
        if match:
            return VERSION_BANNER.format(version=match.group(1))
    raise RuntimeError("unable to locate the TDgpt version information")


class VersionAction(argparse.Action):
    """Resolve the installed version only when the version flag is requested."""

    def __init__(self, option_strings, dest=argparse.SUPPRESS, default=argparse.SUPPRESS, **kwargs):
        super().__init__(option_strings, dest, nargs=0, default=default, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        try:
            banner = get_version_banner()
        except RuntimeError as exc:
            parser.error(str(exc))
        print(banner)
        parser.exit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TDgpt lifecycle and resource manager")
    parser.add_argument("-V", "--version", action=VersionAction, help="show TDgpt version and exit")
    parser.add_argument("scope", choices=["service", "model", "resource"])
    parser.add_argument("action")
    parser.add_argument("target", nargs="?")
    parser.add_argument("--package", action="append", default=[])
    parser.add_argument("--expected-sha256", default="")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--config", default="")
    parser.add_argument("--log-level", default="")
    parser.add_argument("--foreground", action="store_true")
    parser.add_argument("--full-preflight", action="store_true")
    parser.add_argument("--python-runtime-dir", default="")
    parser.add_argument("--release", default="1.0.0")
    parser.add_argument("--resource-base-url", default="")
    parser.add_argument("--format", choices=["table", "json"], default="table")
    return parser


def _add_optional_arg(translated: list[str], arg_name: str, arg_value: str | bool) -> None:
    if arg_value:
        if isinstance(arg_value, bool):
            translated.append(arg_name)
        else:
            translated.extend([arg_name, arg_value])


def translate_args(args: argparse.Namespace) -> list[str]:
    command = COMMANDS.get((args.scope, args.action))
    if command is None:
        raise ValueError(f"unsupported command: {args.scope} {args.action}")
    translated = [command]
    if args.target:
        translated.append(args.target)
    if args.config:
        translated.extend(["--config", args.config])
    _add_optional_arg(translated, "--log-level", args.log_level)
    if args.scope == "resource" and args.action == "import":
        for package in args.package:
            translated.extend(["--package", package])
        _add_optional_arg(translated, "--expected-sha256", args.expected_sha256)
        _add_optional_arg(translated, "--replace", args.replace)
        _add_optional_arg(translated, "--python-runtime-dir", args.python_runtime_dir)
    if args.scope == "resource" and args.action in {"check", "apply"}:
        _add_optional_arg(translated, "--release", args.release)
        _add_optional_arg(translated, "--resource-base-url", args.resource_base_url)
        _add_optional_arg(translated, "--replace", args.replace)
        _add_optional_arg(translated, "--python-runtime-dir", args.python_runtime_dir)
    if args.scope == "resource" and args.action in {"list", "check", "apply", "import", "verify"} and args.format == "json":
        translated.extend(["--format", "json"])
    # `model status` is the machine-readable liveness source the RESTful service
    # reads before deleting the shared runtime, so it must be able to answer in
    # JSON too. The other model actions have no machine-readable output.
    if args.scope == "model" and args.action == "status" and args.format == "json":
        translated.extend(["--format", "json"])
    if args.scope == "service" and args.action == "wait-ready":
        if args.timeout is not None:
            translated.extend(["--timeout", str(args.timeout)])
    if args.scope == "model" and args.action == "start" and args.timeout is not None:
        translated.extend(["--timeout", str(args.timeout)])
    if args.scope == "service" and args.action == "start":
        _add_optional_arg(translated, "--foreground", args.foreground)
        _add_optional_arg(translated, "--full-preflight", args.full_preflight)
    return translated


def main() -> int:
    args = build_parser().parse_args()
    if args.scope == "resource" and args.action == "import" and not args.package:
        raise SystemExit("resource import requires at least one --package")
    try:
        translated = translate_args(args)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    service_script = Path(__file__).resolve().with_name("taosanode_service.py")
    result = subprocess.run([sys.executable, str(service_script), *translated], check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
