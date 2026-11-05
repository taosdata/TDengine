# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Resumable execution for the large external-source query matrices.
#
# test_vtable_query_ext_source_stb.py expands to ~200 pytest items (22 source
# matrices x 5-14 shared .in files) and each query is orders of magnitude
# slower on Windows than on Linux, so a single run does not fit in a CI case
# timeout.  A timeout used to throw the completed work away and the next run
# started from the first source matrix again.
#
# This conftest records every passed test of that file in a small JSON journal
# and deselects those items on the next run, so an interrupted run resumes at
# the first test it had not finished.  The journal is keyed by the Oracle
# version list and the required-services set, because a different server matrix
# invalidates the vtables the earlier run prepared.
#
# Environment:
#   FQ_STB_RESUME=0             force a full run (progress is still recorded)
#   FQ_STB_PROGRESS_FILE=<path> override the journal location (CI can keep it
#                               in a cache directory between jobs)

import hashlib
import json
import os
import tempfile

_RESUMABLE_MODULE = "test_vtable_query_ext_source_stb.py"


def _journal_path():
    explicit = os.environ.get("FQ_STB_PROGRESS_FILE", "")
    if explicit:
        return explicit
    key = "|".join((
        os.environ.get("FQ_ORACLE_VERSIONS", "default"),
        os.environ.get("FQ_REQUIRED_SERVICES", "all"),
    ))
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:8]
    return os.path.join(tempfile.gettempdir(),
                        "fq-stb-progress-%s.json" % digest)


def _load_progress(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return set()
    passed = data.get("passed") if isinstance(data, dict) else None
    return set(passed) if isinstance(passed, list) else set()


def _save_progress(path, done):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump({"passed": sorted(done)}, handle)
    os.replace(tmp, path)


def _is_resumable(item):
    return item.nodeid.split("::", 1)[0].replace("\\", "/").endswith(
        _RESUMABLE_MODULE)


def pytest_collection_modifyitems(config, items):
    if os.environ.get("FQ_STB_RESUME", "1") == "0":
        return
    resumable = [item for item in items if _is_resumable(item)]
    if not resumable:
        return
    path = _journal_path()
    done = _load_progress(path)
    if not done:
        return
    remaining = [item for item in items
                 if not (_is_resumable(item) and item.nodeid in done)]
    skipped = [item for item in items if item not in remaining]
    if not skipped:
        return
    config.hook.pytest_deselected(items=skipped)
    items[:] = remaining
    reporter = config.pluginmanager.get_plugin("terminalreporter")
    message = ("[stb-resume] journal %s: %d item(s) already passed, "
               "%d remaining in this run"
               % (path, len(skipped), len(remaining)))
    if reporter is not None:
        reporter.write_line(message)
    else:
        print(message)


def pytest_runtest_logreport(report):
    if report.when != "call" or not report.passed:
        return
    if not report.nodeid.split("::", 1)[0].replace("\\", "/").endswith(
            _RESUMABLE_MODULE):
        return
    path = _journal_path()
    done = _load_progress(path)
    if report.nodeid in done:
        return
    done.add(report.nodeid)
    try:
        _save_progress(path, done)
    except OSError:
        # A read-only workspace must not turn a passing test into an error;
        # the next run simply starts from the first unrecorded item.
        pass
