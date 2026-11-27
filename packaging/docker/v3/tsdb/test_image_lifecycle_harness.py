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

"""Failure-path regressions for the Docker acceptance harness; no daemon needed."""
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import test_image_lifecycle as lifecycle


class LifecycleCleanupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # A private subclass prevents tests sharing the real suite's state.
        class Harness(lifecycle.ImageLifecycleTests):
            next_container = 0
        self.case = Harness("test_all_services_disabled_fails")
        self.case.run_id = "cleanup-regression"
        self.case.image = "test-image"
        self.case.containers = []
        self.case.report_dir = Path(self.tmp.name)
        self.case.docker = Mock(side_effect=self.docker)
        self.present = True
        self.label = self.case.run_id
        self.running = True

    def docker(self, *args, **kwargs):
        if args[0] == "inspect":
            if not self.present:
                return subprocess.CompletedProcess(args, 1, "", "Error: No such object: " + args[1])
            details = {"Config": {"Labels": {"tdengine.lifecycle-test": self.label}},
                       "State": {"Running": self.running}}
            return subprocess.CompletedProcess(args, 0, json.dumps([details]), "")
        return subprocess.CompletedProcess(args, 0, "container log", "")

    def test_start_failure_after_creation_is_cleaned(self):
        def docker(*args, **kwargs):
            if args[0] == "run":
                raise subprocess.CalledProcessError(125, args, stderr="OCI runtime error")
            return self.docker(*args, **kwargs)
        self.case.docker.side_effect = docker
        with self.assertRaises(subprocess.CalledProcessError):
            self.case.new_container()
        name, = self.case.containers
        self.case.tearDown()
        self.case.docker.assert_any_call("stop", "-t", "30", name)
        self.case.docker.assert_any_call("rm", "-v", name)
        self.assertEqual(self.case.containers, [])

    def test_run_timeout_keeps_container_tracked(self):
        self.case.docker.side_effect = subprocess.TimeoutExpired("docker run", 60)
        with self.assertRaises(subprocess.TimeoutExpired):
            self.case.new_container()
        self.assertEqual(len(self.case.containers), 1)
        self.case.docker.side_effect = self.docker
        self.case.tearDown()
        self.assertEqual(self.case.containers, [])

    def test_failure_before_creation_has_nothing_to_remove(self):
        self.case.containers.append("missing")
        self.present = False
        self.case.tearDown()
        self.assertEqual(self.case.containers, [])
        self.assertEqual([call.args[0] for call in self.case.docker.call_args_list], ["inspect"])

    def test_report_write_failure_still_cleans_all_containers(self):
        self.case.containers.extend(["first", "second"])
        with patch.object(Path, "write_text", side_effect=OSError("No space left on device")):
            with self.assertRaisesRegex(RuntimeError, "No space left on device"):
                self.case.tearDown()
        for name in ("first", "second"):
            self.case.docker.assert_any_call("rm", "-v", name)
        self.assertEqual(self.case.containers, [])

    def test_one_stop_failure_forces_removal_and_does_not_skip_others(self):
        self.case.containers.extend(["first", "second"])
        def docker(*args, **kwargs):
            if args[0] == "stop" and args[-1] == "first":
                raise subprocess.CalledProcessError(1, args)
            return self.docker(*args, **kwargs)
        self.case.docker.side_effect = docker
        with self.assertRaisesRegex(RuntimeError, "first"):
            self.case.tearDown()
        self.case.docker.assert_any_call("rm", "-f", "-v", "first")
        self.case.docker.assert_any_call("rm", "-v", "second")
        self.assertEqual(self.case.containers, [])

    def test_failed_forced_removal_keeps_container_tracked_and_reports_both_errors(self):
        self.case.containers.extend(["first", "second"])
        def docker(*args, **kwargs):
            if args[0] == "stop" and args[-1] == "first":
                raise subprocess.CalledProcessError(1, args)
            if args[:3] == ("rm", "-f", "-v") and args[-1] == "first":
                raise subprocess.CalledProcessError(1, args)
            return self.docker(*args, **kwargs)
        self.case.docker.side_effect = docker
        with self.assertRaisesRegex(RuntimeError, "first.*docker stop failed.*docker rm failed"):
            self.case.tearDown()
        self.case.docker.assert_any_call("rm", "-f", "-v", "first")
        self.case.docker.assert_any_call("rm", "-v", "second")
        self.assertEqual(self.case.containers, ["first"])

    def test_foreign_label_is_never_removed(self):
        self.case.containers.append("foreign")
        self.label = "another-run"
        with self.assertRaises(RuntimeError):
            self.case.tearDown()
        self.assertEqual([call.args[0] for call in self.case.docker.call_args_list], ["inspect"])
        self.assertEqual(self.case.containers, ["foreign"])

    def test_daemon_error_is_reported_not_treated_as_missing(self):
        self.case.containers.append("unknown")
        self.case.docker.return_value = subprocess.CompletedProcess("inspect", 1, "", "Cannot connect to daemon")
        self.case.docker.side_effect = None
        with self.assertRaises(RuntimeError):
            self.case.tearDown()
        self.assertEqual(self.case.containers, ["unknown"])
        self.assertEqual(self.case.docker.call_count, 1)


if __name__ == "__main__":
    unittest.main()
