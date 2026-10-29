# Copyright (c) TAOS Data, Inc. All rights reserved.
#
# SPDX-License-Identifier: LicenseRef-TAOSData-Commercial
#
# This file is proprietary to TAOS Data, Inc. and may contain confidential
# information. No rights are granted by this file. Use is permitted only
# under a written agreement with TAOS Data, Inc. or an authorized affiliate.
# Unauthorized copying, modification, distribution, or disclosure, in whole
# or in part, by any medium, is strictly prohibited.

"""Opt-in lifecycle acceptance tests for a locally built image (Docker required)."""
import argparse
import json
import subprocess
import time
import unittest
import uuid
from pathlib import Path


class ImageLifecycleTests(unittest.TestCase):
    image = None
    edition = None
    report_dir = None

    @classmethod
    def setUpClass(cls):
        if not cls.image:
            raise unittest.SkipTest("Run explicitly with --image, --edition and --report-dir")
        cls.services = ["taosd", "taosadapter", "taoskeeper", "taos-explorer"]
        if cls.edition == "enterprise":
            cls.services.append("taosx")
        cls.run_id = "tsdb-lifecycle-" + uuid.uuid4().hex[:12]
        cls.report_dir.mkdir(parents=True, exist_ok=False)
        cls.containers = []
        cls.next_container = 0
        metadata = cls.docker("image", "inspect", cls.image)
        (cls.report_dir / "image.json").write_text(metadata.stdout)
        cls.events = []

    @staticmethod
    def docker(*args, check=True, timeout=60):
        return subprocess.run(["docker", *args], capture_output=True, text=True,
                              check=check, timeout=timeout)

    def record(self, case, **fields):
        self.events.append(dict(case=case, **fields))
        (self.report_dir / "results.json").write_text(json.dumps(self.events, indent=2))

    def state(self, name):
        return json.loads(self.docker("inspect", name).stdout)[0]

    def new_container(self, *options, command=()):
        name = self.run_id + "-" + str(type(self).next_container)
        type(self).next_container += 1
        # Docker may create a container before reporting a start failure.
        self.containers.append(name)
        self.docker("run", "-d", "--name", name, "--hostname", name,
                    "--label", "tdengine.lifecycle-test=" + self.run_id,
                    *options, self.image, *command)
        return name

    def wait_until(self, predicate, seconds=120):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.5)
        self.fail("Timed out after {}s".format(seconds))

    def ready(self, name, services=None):
        expected = self.services if services is None else services

        def check():
            state = self.state(name)["State"]
            if not state["Running"]:
                self.fail("Container exited before ready: " + self.docker("logs", name).stdout)
            log = self.docker("logs", name).stdout
            if "All services started, watching child processes:" not in log:
                return False
            for service in expected:
                if self.docker("exec", name, "pgrep", "-x", service, check=False).returncode:
                    return False
            result = self.docker("exec", name, "taos", "-s", "select server_version();", check=False)
            return result.returncode == 0 and "DB error" not in result.stdout + result.stderr

        self.wait_until(check)

    def sql(self, name, query):
        result = self.docker("exec", name, "taos", "-s", query)
        self.assertNotIn("DB error", result.stdout + result.stderr)
        return result.stdout

    def remove(self, name):
        # Only exact names recorded by this run, with its label, are eligible.
        self.assertIn(name, self.containers)
        inspected = self.docker("inspect", name, check=False)
        if inspected.returncode:
            # A run rejected before creation has nothing to remove. Other errors
            # (including a daemon outage) must remain visible and retryable.
            if "No such object:" in inspected.stderr or "No such container:" in inspected.stderr:
                self.containers.remove(name)
                return
            inspected.check_returncode()
        details = json.loads(inspected.stdout)[0]
        self.assertEqual(details["Config"]["Labels"].get("tdengine.lifecycle-test"), self.run_id)
        report_errors = []
        try:
            logs = self.docker("logs", "--timestamps", name, check=False)
            (self.report_dir / (name + ".log")).write_text(logs.stdout + logs.stderr)
            (self.report_dir / (name + ".json")).write_text(json.dumps(details, indent=2))
        except Exception as exc:
            report_errors.append(str(exc))
        finally:
            cleanup_errors = []
            if details["State"]["Running"]:
                try:
                    self.docker("stop", "-t", "30", name)
                except Exception as exc:
                    cleanup_errors.append("docker stop failed: " + str(exc))
            # -v removes only anonymous volumes belonging to this test container.
            # If graceful stop failed, force removal so a stuck test container
            # cannot leak into later runs. The exact name and run label were
            # verified above before either cleanup command is attempted.
            remove_args = ("rm", "-f", "-v", name) if cleanup_errors else ("rm", "-v", name)
            try:
                self.docker(*remove_args)
            except Exception as exc:
                cleanup_errors.append("docker rm failed: " + str(exc))
            else:
                self.containers.remove(name)
        errors = []
        if cleanup_errors:
            errors.append("cleanup failed: " + "; ".join(cleanup_errors))
        if report_errors:
            errors.append("report collection failed: " + "; ".join(report_errors))
        if errors:
            raise RuntimeError("Container removal/reporting errors: " + "; ".join(errors))

    def tearDown(self):
        errors = []
        for name in list(self.containers):
            try:
                self.remove(name)
            except Exception as exc:
                errors.append(name + ": " + str(exc))
        if errors:
            raise RuntimeError("Cleanup errors: " + "; ".join(errors))

    def test_image_installation(self):
        metadata = json.loads((self.report_dir / "image.json").read_text())[0]["Config"]
        self.assertEqual(metadata["Entrypoint"],
                         ["/usr/bin/tini", "--", "/usr/bin/entrypoint.sh"])
        for volume in ("/var/lib/taos", "/var/log/taos"):
            self.assertIn(volume, metadata["Volumes"])
        name = self.new_container(
            "--entrypoint", "/bin/sh",
            command=("-ec", 'for binary do test -x "/usr/bin/$binary"; done; '
                     'test -d /docker-entrypoint-initdb.d; ldd /usr/bin/taosd',
                     "installation-smoke", "tini", "entrypoint.sh", "taos-check", "taos", *self.services))
        self.wait_until(lambda: not self.state(name)["State"]["Running"])
        self.assertEqual(self.state(name)["State"]["ExitCode"], 0)
        self.assertNotIn("not found", self.docker("logs", name).stdout)
        # Check the installed files without starting database services. Using
        # docker cp also works for this deliberately stopped test container.
        for filename in ("entrypoint.sh", "taos-check"):
            target = self.report_dir / (name + "-" + filename)
            self.docker("cp", name + ":/usr/bin/" + filename, str(target))
            self.assertEqual(target.read_bytes(), (Path(__file__).resolve().parent / filename).read_bytes())
        self.record("image-installation", result="passed", entrypoint=metadata["Entrypoint"])

    def test_failed_container_start_is_cleaned(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.new_container("--entrypoint", "/nonexistent-lifecycle-entrypoint")
        name = self.containers[-1]
        details = self.state(name)
        self.assertEqual(details["State"]["Status"], "created")
        volumes = [mount["Name"] for mount in details["Mounts"] if mount["Type"] == "volume"]
        self.remove(name)
        self.assertNotEqual(self.docker("inspect", name, check=False).returncode, 0)
        for volume in volumes:
            self.assertNotEqual(self.docker("volume", "inspect", volume, check=False).returncode, 0)
        self.record("failed-start-cleanup", result="passed", anonymous_volumes_removed=len(volumes))

    def test_all_services_and_clean_stop(self):
        name = self.new_container()
        self.ready(name)
        if self.edition == "oss":
            self.assertNotEqual(self.docker("exec", name, "sh", "-c", "command -v taosx", check=False).returncode, 0)
        self.sql(name, "create database lifecycle_smoke; create table lifecycle_smoke.t(ts timestamp, v int); "
                       "insert into lifecycle_smoke.t values(now, 42); select * from lifecycle_smoke.t;")
        self.docker("stop", "-t", "30", name)
        self.assertEqual(self.state(name)["State"]["ExitCode"], 0)
        self.docker("start", name)
        self.ready(name)
        self.assertIn("42", self.sql(name, "select * from lifecycle_smoke.t;"))
        self.docker("stop", "-t", "30", name)
        self.assertEqual(self.state(name)["State"]["ExitCode"], 0)
        self.record("startup-sql-clean-stop-start", result="passed", services=self.services)

    def test_each_resident_service_exit_fails_container(self):
        for service in self.services:
            with self.subTest(service=service):
                name = self.new_container()
                self.ready(name)
                self.docker("exec", name, "pkill", "-KILL", "-x", service)
                self.wait_until(lambda: not self.state(name)["State"]["Running"], seconds=25)
                state = self.state(name)["State"]
                self.assertEqual(state["ExitCode"], 1)
                self.assertFalse(state["OOMKilled"])
                self.record("service-exit", service=service, result="passed", exit_code=state["ExitCode"])
                self.remove(name)

    def test_restart_policy_recovers_services_and_data(self):
        name = self.new_container("--restart", "unless-stopped")
        self.ready(name)
        self.sql(name, "create database lifecycle_restart; create table lifecycle_restart.t(ts timestamp, v int); "
                       "insert into lifecycle_restart.t values(now, 73);")
        self.docker("exec", name, "pkill", "-KILL", "-x", "taosadapter")
        self.wait_until(lambda: self.state(name)["RestartCount"] >= 1, seconds=40)
        self.ready(name)
        self.assertIn("73", self.sql(name, "select * from lifecycle_restart.t;"))
        self.docker("stop", "-t", "30", name)
        self.assertEqual(self.state(name)["State"]["ExitCode"], 0)
        restarts = self.state(name)["RestartCount"]
        time.sleep(2)
        self.assertFalse(self.state(name)["State"]["Running"])
        self.assertEqual(self.state(name)["RestartCount"], restarts)
        self.record("restart-policy-data-stop", result="passed", restart_count=restarts)

    def test_disabled_optional_services_are_not_required(self):
        options = []
        for key in ("ADAPTER", "KEEPER", "TAOSX", "EXPLORER"):
            options.extend(["-e", "TAOS_DISABLE_" + key + "=1"])
        name = self.new_container(*options)
        self.ready(name, services=["taosd"])
        for service in self.services:
            if service != "taosd":
                self.assertNotEqual(self.docker("exec", name, "pgrep", "-x", service, check=False).returncode, 0)
        self.docker("stop", "-t", "30", name)
        self.assertEqual(self.state(name)["State"]["ExitCode"], 0)
        self.record("disabled-optional-services", result="passed")

    def test_all_services_disabled_fails(self):
        options = []
        for key in ("TAOSD", "ADAPTER", "KEEPER", "TAOSX", "EXPLORER"):
            options.extend(["-e", "TAOS_DISABLE_" + key + "=1"])
        name = self.new_container(*options)
        self.wait_until(lambda: not self.state(name)["State"]["Running"], seconds=20)
        self.assertEqual(self.state(name)["State"]["ExitCode"], 1)
        self.assertIn("No TDengine services were started", self.docker("logs", name).stdout)
        self.record("all-services-disabled", result="passed", exit_code=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--edition", required=True, choices=("enterprise", "oss"))
    parser.add_argument("--report-dir", required=True, type=Path, help="A new directory for logs and results")
    args = parser.parse_args()
    ImageLifecycleTests.image = args.image
    ImageLifecycleTests.edition = args.edition
    ImageLifecycleTests.report_dir = args.report_dir
    unittest.main(argv=[__file__, "-v"])
