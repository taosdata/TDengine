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

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("windows_ci", Path(__file__).with_name("run_windows_ci.py"))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


class SourceSyncTest(unittest.TestCase):
    def git(self, directory, *args):
        result = subprocess.run(["git", "-C", str(directory), *args], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def test_changing_shallow_runner_and_broken_workspace(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            upstream, runner, workspace = (base / name for name in ("upstream", "runner", "workspace"))
            upstream.mkdir()
            self.git(upstream, "init")
            self.git(upstream, "config", "user.name", "Fixture")
            self.git(upstream, "config", "user.email", "fixture@example.invalid")
            (upstream / ".gitignore").write_text(".externals/\ndebug/\n")
            commits = []
            for value in ("main", "3.0", "new MR"):
                (upstream / "source.txt").write_text(value)
                self.git(upstream, "add", ".")
                self.git(upstream, "commit", "-m", value)
                commits.append(self.git(upstream, "rev-parse", "HEAD"))
            subprocess.run(["git", "clone", "--depth=1", upstream.as_uri(), str(runner)], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            workspace.mkdir()
            cache = workspace / ".externals" / "retained"
            cache.parent.mkdir()
            cache.write_text("cache")
            # An incomplete directory left by a previous interrupted cleanup.
            (workspace / "source").mkdir()
            for index in (0, 1, 2, 0):
                sha = commits[index]
                self.git(runner, "fetch", "--depth=1", upstream.as_uri(), sha)
                self.git(runner, "checkout", "--detach", "-f", sha)
                with patch.dict(os.environ, CI_PROJECT_DIR=str(runner), CI_COMMIT_SHA=sha):
                    ci.sync_source(workspace)
                self.assertEqual(self.git(workspace, "rev-parse", "HEAD"), sha)
                self.assertEqual((workspace / "source.txt").read_text(), ("main", "3.0", "new MR")[index])
                self.assertEqual(cache.read_text(), "cache")
                self.git(workspace, "fsck", "--connectivity-only")
                # Corrupt the persistent metadata: next sync must replace it.
                (workspace / ".git" / "shallow").unlink(missing_ok=True)
            before = self.git(workspace, "rev-parse", "HEAD")
            with patch.dict(os.environ, CI_PROJECT_DIR=str(runner), CI_COMMIT_SHA="f" * 40):
                with self.assertRaises(RuntimeError):
                    ci.sync_source(workspace)
            self.assertEqual(self.git(workspace, "rev-parse", "HEAD"), before)
            self.assertEqual(cache.read_text(), "cache")

    def test_preparation_failure_does_not_collect_old_reports(self):
        with patch.object(sys, "argv", ["run_windows_ci.py"]), \
             patch.object(ci, "print_host_identity"), \
             patch.object(ci, "build", side_effect=RuntimeError("checkout failed")), \
             patch.object(ci, "stop_processes"), patch.object(ci, "stop_build_processes"), \
             patch.object(ci, "collect_artifacts") as collect:
            self.assertEqual(ci.main(), 1)
            collect.assert_not_called()

    def test_runner_checkout_is_not_modified(self):
        with tempfile.TemporaryDirectory() as temporary, \
             patch.dict(os.environ, CI_PROJECT_DIR=temporary, CI_COMMIT_SHA="a" * 40):
            with self.assertRaisesRegex(RuntimeError, "separate"):
                ci.sync_source(Path(temporary))


if __name__ == "__main__":
    unittest.main()
