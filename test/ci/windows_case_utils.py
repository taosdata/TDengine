# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

import hashlib
import json
import os
import re
import shlex
import sys
from fnmatch import fnmatchcase


def is_pytest_collection_skip(return_code, output):
    """Recognize pytest module skips without accepting an empty test selection."""
    return return_code == 5 and re.search(
        r'\bcollected 0 items / [1-9][0-9]* skipped\b', output
    ) is not None


def is_pytest_only_skip(return_code, output):
    """Do not count a command with only skipped tests as successful coverage."""
    if is_pytest_collection_skip(return_code, output):
        return True
    return return_code == 0 and re.search(
        r"^=+\s+[1-9][0-9]* skipped(?:, [1-9][0-9]* warnings?)? in [^\n]+?=+\s*\Z",
        output, re.MULTILINE,
    ) is not None


FQ_SMOKE_CASES = (
    "cases/09-DataQuerying/19-FederatedQuery/test_fq_01_external_source.py",
    "cases/09-DataQuerying/19-FederatedQuery/test_fq_20_sqlserver_semantics.py",
    "cases/05-VirtualTables/04-Query/03-ExtSource/test_vtable_query_ext_source_filter.py",
)

# The federated-query inventory: the FQ suite plus the external-source
# virtual-table, auth and plan suites.  WIN_ONLY_FQ selects exactly these.
FQ_CASE_MARKERS = (
    "cases/09-DataQuerying/19-FederatedQuery/",
    "cases/05-VirtualTables/01-DDL/03-ExtSource/",
    "cases/05-VirtualTables/04-Query/03-ExtSource/",
    "cases/05-VirtualTables/02-Auth/test_vtable_auth_ext_source.py",
    "cases/05-VirtualTables/05-Plan/test_vtable_plan_pushdown_agg_ext_source.py",
    "cases/05-VirtualTables/05-Plan/test_vtable_plan_pushdown_ext_source_ts_precision.py",
    "cases/05-VirtualTables/05-Plan/test_vtable_plan_window_ext_source.py",
    "cases/05-VirtualTables/05-Plan/test_vtable_plan_agg_federated.py",
    "cases/05-VirtualTables/05-Plan/test_vtable_plan_window_federated_batch.py",
    # The Windows Oracle installer check is part of the FQ inventory too.
    "packaging/test_fq_oracle_install_windows.py",
)

# Cases that directly exercise the Oracle-on-Windows work (and the FQ/vtable
# helpers it touches).  WIN_CASE_SUBSET=related selects exactly these for a
# focused verification run.
RELATED_CASE_MARKERS = (
    "19-FederatedQuery/test_fq_01_external_source.py",
    "19-FederatedQuery/test_fq_05_config_effectiveness.py",
    "19-FederatedQuery/test_fq_09_stability.py",
    "19-FederatedQuery/test_fq_19_runtime_libs.py",
    "19-FederatedQuery/test_fq_22_runtime_package.py",
    "04-Query/03-ExtSource/test_vtable_query_ext_source_filter.py",
    "04-Query/03-ExtSource/test_vtable_query_ext_source_runtime_libs.py",
    "packaging/test_fq_oracle_install_windows.py",
    "81-Tools/04-Taosdump/",
)


def is_fq_command(command):
    """True when the command runs a federated-query case."""
    if command.lstrip().startswith("#"):
        # A commented-out row is part of the file's prose, not the selection.
        return False
    normalized = command.replace("\\", "/")
    return any(marker in normalized for marker in FQ_CASE_MARKERS)


def is_related_command(command):
    """True when the command runs a case related to the local branch changes."""
    if command.lstrip().startswith("#"):
        return False
    normalized = command.replace("\\", "/")
    return any(marker in normalized for marker in RELATED_CASE_MARKERS)


def selection_contains_fq(contents):
    """True when a manifest selection includes at least one FQ case."""
    return any(is_fq_command(line) for line in contents.splitlines())


def windows_task_manifest(ci_dir, suite="smoke"):
    """Keep the small default gate separate from the full Windows inventory."""
    smoke = (ci_dir / "win_cases.task").read_text(encoding="utf-8")
    if suite == "smoke":
        return smoke
    if suite == "full":
        return (ci_dir / "cases.task").read_text(encoding="utf-8") + "\n" + smoke
    raise ValueError(f"Unknown Windows test suite: {suite}")


def is_excluded_command(command, exclusion_list, suite="full"):
    """Match files, pytest node IDs, directory prefixes, and glob patterns."""
    command = re.sub(r",[0-9]+\s*$", "", command)
    arguments = shlex.split(command)
    if suite == "smoke" and any(argument in FQ_SMOKE_CASES for argument in arguments):
        return False
    for argument in arguments:
        path = argument.replace("\\", "/").split("::", 1)[0]
        for pattern in exclusion_list:
            if (path.startswith(pattern) if pattern.endswith("/") else fnmatchcase(path, pattern)):
                return True
    return False


def parse_case_line(line):
    """Keep commas in the command and remove the Linux pytest wrapper."""
    parts = line.split(",", 4)
    if len(parts) != 5 or parts[2].strip() not in {"y", "n"}:
        raise ValueError(f"Invalid task row: {line}")
    command = parts[4].strip()
    legacy_timeout = re.search(r",([0-9]+)$", command)
    if legacy_timeout:
        command = command[:legacy_timeout.start()].rstrip()
    if "ci/pytest.sh " in command:
        pytest_cmd = command.split("ci/pytest.sh ", 1)[1]
    else:
        pytest_cmd = re.sub(r"^CASE_TIMEOUT=[0-9]+\s+", "", command)
    if not pytest_cmd.startswith("pytest "):
        raise ValueError(f"Unsupported Windows task command: {command}")
    timeout = 1800
    # This command runs several native/REST workloads, including 100 SQLs
    # across multiple consumers. Windows can keep making progress past 30 min.
    if "cases/81-Tools/03-Benchmark/test_benchmark_query_rest.py" in shlex.split(pytest_cmd):
        timeout = 7200
    if legacy_timeout:
        timeout = int(legacy_timeout.group(1))
    for argument in shlex.split(command):
        if argument.startswith("CASE_TIMEOUT="):
            timeout = int(argument.split("=", 1)[1])
    return pytest_cmd, timeout


def case_log_prefix(command):
    """Case-file part of a pytest log name (the part before the env hash)."""
    return shlex.split(command)[1].rsplit("/", 1)[-1].split("::", 1)[0]


def pytest_log_name(command, environment=None):
    """Use a bounded Windows-safe name that distinguishes pytest selectors."""
    case_file = case_log_prefix(command)
    identity = command
    if environment:
        identity += "\0" + json.dumps(environment, sort_keys=True)
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"{case_file}-{digest}.log"


def pytest_command_args(command, timeout=None):
    """Preserve quoted pytest arguments without passing through cmd.exe."""
    arguments = shlex.split(command)
    # Override pytest.ini's 1200s default as well as the outer process deadline.
    # Keep timeout options explicitly supplied by the testcase registration.
    if timeout is not None and not any(
        argument == "--timeout" or argument.startswith("--timeout=")
        for argument in arguments
    ):
        arguments.append(f"--timeout={timeout}")
    result = [sys.executable, "-m", *arguments]
    # --clean is registered by test/conftest.py.  A case outside the test tree
    # (the packaging installer check lives in ../packaging) is collected with
    # rootdir=source/taos-community, where that conftest is never loaded, so
    # pytest rejects the option ("unrecognized arguments: --clean").
    if len(arguments) < 2 or not (arguments[1].startswith("..") or os.path.isabs(arguments[1])):
        result.append("--clean")
    return result


def resume_task_manifest(contents, checkpoint):
    """Skip only exact completed commands, including their pytest arguments."""
    completed = set()
    for record in checkpoint["completed"]:
        if record["status"] not in {"Success", "Failed"}:
            raise ValueError(f"Invalid resume status: {record['status']}")
        command = tuple(shlex.split(record["command"]))
        if len(command) < 2 or command[0] != "pytest":
            raise ValueError(f"Invalid resume command: {record['command']}")
        completed.add(command)
    retained = []
    matched = set()
    for line in contents.splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            try:
                command, _ = parse_case_line(line)
            except ValueError:
                # Non-pytest tasks still need the normal platform exclusion pass.
                retained.append(line)
                continue
            key = tuple(shlex.split(command))
            if key in completed:
                matched.add(key)
                continue
        retained.append(line)
    if completed != matched:
        raise ValueError("Resume checkpoint contains commands absent from this manifest")
    return "\n".join(retained) + "\n", len(matched)
