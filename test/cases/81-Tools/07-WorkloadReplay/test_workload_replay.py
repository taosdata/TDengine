# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import urllib.error
import urllib.request

import pytest

from new_test_framework.utils import tdLog, etool


class TestWorkloadReplay:
    """Replay a recorded workload with taosreplay and check the replay report.

    A scenario that declares a recording source is required to run: a missing local
    file or a failed download fails the case. Only a scenario with no source at all
    (or a missing taosreplay binary) skips, so a provisioned CI run cannot silently
    stop replaying while still reporting success.
    """

    SCENARIO_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wrk-scenarios.json")
    REPORT_FILE = "replay-report.json"
    # CI kills a case at CASE_TIMEOUT (1200 s unless the task raises it), so keep this
    # below that: a timeout enforced here still produces a log and a failure, whereas an
    # outside kill leaves nothing behind. Raise both together when amplifying a replay.
    REPLAY_TIMEOUT_SEC = int(os.environ.get("WLR_REPLAY_TIMEOUT_SEC", "900"))

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.scenario_name, cls.scenario = cls.load_scenario()
        cls.sample = os.environ.get(cls.scenario.get("sample", ""), "")
        # Keep artifacts out of the case directory: <test>/output/workload-replay/<scenario>
        test_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
        cls.report_dir = os.path.join(test_root, "output", "workload-replay", cls.scenario_name)
        os.makedirs(cls.report_dir, exist_ok=True)

    # ------------------------------------------------------------------ util
    @staticmethod
    def load_catalog():
        with open(TestWorkloadReplay.SCENARIO_FILE, encoding="utf-8") as handle:
            return json.load(handle)["scenarios"]

    @staticmethod
    def load_scenario():
        """Resolve the scenario to replay: WLR_REPLAY_SCENARIO, else the catalog default."""
        with open(TestWorkloadReplay.SCENARIO_FILE, encoding="utf-8") as handle:
            catalog = json.load(handle)
        name = os.environ.get("WLR_REPLAY_SCENARIO", catalog["default"])
        scenarios = catalog["scenarios"]
        assert name in scenarios, f"unknown scenario {name}; known: {sorted(scenarios)}"
        return name, scenarios[name]

    def require_sample(self):
        resolved = self.sample_path(self.scenario_name, self.scenario)
        if resolved:
            self.sample = resolved
            return
        local = os.environ.get(self.scenario.get("sample", ""), "")
        if local:
            raise AssertionError(f"scenario {self.scenario_name}: recording not found at {local}")
        pytest.skip(
            f"scenario {self.scenario_name}: no recording configured; set "
            f"{self.scenario.get('sample')} to a local .wlr or give the scenario a sampleUrl. "
            f"{self.scenario.get('sampleHint', '')}"
        )

    def sample_path(self, name, scenario):
        """Resolve a scenario's recording to a local file, downloading it when needed.

        An empty result means the scenario declares no source at all. A scenario that
        declares one is provisioned on purpose, so a missing local file or a failed
        download raises instead of returning empty.
        """
        local = os.environ.get(scenario.get("sample", ""), "")
        if local:
            assert os.path.isfile(local), f"scenario {name}: recording not found at {local}"
            return local
        return self.fetch_sample(name, scenario)

    def sample_url(self, name, scenario):
        """Resolve where a scenario's recording is hosted.

        A recording is too large to keep in the repository, so CI fetches it from the
        package NAS. The WLR_REPLAY_SAMPLE_URL override applies to the selected scenario
        only, never to a bootstrap recording; the template's {nas_root} is replaced with
        TSDB_NAS_ROOT (or the internal default). An empty result means no source.
        """
        if name == self.scenario_name:
            override = os.environ.get("WLR_REPLAY_SAMPLE_URL", "")
            if override:
                return override
        template = scenario.get("sampleUrl", "")
        if not template:
            return ""
        nas_root = os.environ.get("TSDB_NAS_ROOT", "http://192.168.1.131/data/nas/TDengine/")
        return template.format(nas_root=nas_root.rstrip("/") + "/")

    def fetch_sample(self, name, scenario):
        url = self.sample_url(name, scenario)
        if not url:
            return ""
        cache_dir = os.path.join(os.path.dirname(self.report_dir), "samples")
        os.makedirs(cache_dir, exist_ok=True)
        target = os.path.join(cache_dir, f"{name}.wlr")
        expected = scenario.get("sampleSha256", "")
        if name == self.scenario_name:
            expected = os.environ.get("WLR_REPLAY_SAMPLE_SHA256", "") or expected

        if os.path.isfile(target) and expected and self.sha256(target) == expected:
            tdLog.info(f"reusing cached recording {target}")
            return target

        tdLog.info(f"downloading recording {url}")
        try:
            with urllib.request.urlopen(url, timeout=300) as response, open(target + ".part", "wb") as handle:
                while True:
                    chunk = response.read(1 << 20)
                    if not chunk:
                        break
                    handle.write(chunk)
        except (urllib.error.URLError, OSError) as err:
            # The source was declared on purpose, so an unfetchable recording is a broken
            # provision (NAS down, file moved) and has to fail: skipping would keep CI green
            # while the case silently stopped replaying anything.
            raise AssertionError(f"cannot download recording from {url}: {err}") from err
        os.replace(target + ".part", target)

        if expected:
            actual = self.sha256(target)
            assert actual == expected, f"sha256 mismatch for {target}: {actual} != {expected}"
        return target

    @staticmethod
    def sha256(path):
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
        return digest.hexdigest()

    def require_replayer(self):
        binary = self.resolve_replayer()
        if not binary:
            pytest.skip("taosreplay not found; build the taos-community tools or set TAOSREPLAY_BIN")
        return binary

    @staticmethod
    def resolve_replayer():
        """Locate the taosreplay binary.

        The framework's bin path is derived from the deployed cluster and does not always
        point at a sibling tool, so try an explicit override first and then the usual
        neighbours of the taosd that the framework did resolve.
        """
        candidates = []
        override = os.environ.get("TAOSREPLAY_BIN", "")
        if override:
            candidates.append(override)
        for framework_bin in ("taosreplay", "taosd"):
            try:
                candidate = etool.binFile(framework_bin)
            except Exception:  # noqa: BLE001 - the framework path is best-effort only
                continue
            if framework_bin == "taosd":
                candidate = os.path.join(os.path.dirname(candidate), "taosreplay")
            candidates.append(candidate)
        found = shutil.which("taosreplay")
        if found:
            candidates.append(found)
        for candidate in candidates:
            if candidate and os.path.isfile(candidate):
                return candidate
        return ""

    def build_command(self, binary, sample=None, scenario=None, report_dir=None):
        scenario = scenario if scenario is not None else self.scenario
        sample = sample or self.sample
        report_dir = report_dir or self.report_dir
        parts = [
            binary,
            "replay",
            f'--input "{sample}"',
            "--user-password root=taosdata",
            f"--time-scale {scenario['timeScale']}",
            f'--report-dir "{report_dir}"',
        ]
        if scenario.get("allowAsyncOverlap"):
            parts.append("--allow-async-overlap")
        # A recording stopped without a session-stop marker is still structurally
        # complete; --allow-incomplete accepts it instead of refusing the whole file.
        if scenario.get("allowIncomplete"):
            parts.append("--allow-incomplete")
        # Replay against the deployed cluster by default; a dedicated endpoint lets the
        # case run against an isolated instance that the recording may safely recreate.
        # before_test_class publishes the deployed endpoint as self.host / self.port.
        target_port = os.environ.get("WLR_REPLAY_TARGET_PORT", "") or str(getattr(self, "port", "") or "")
        if target_port:
            target_host = os.environ.get("WLR_REPLAY_TARGET_HOST", "") or getattr(self, "host", "") or "127.0.0.1"
            parts.extend([f"--target-host {target_host}", f"--target-port {target_port}"])
        return " ".join(parts[1:])

    def read_report(self, report_dir=None):
        path = os.path.join(report_dir or self.report_dir, self.REPORT_FILE)
        assert os.path.isfile(path), f"replay report missing at {path}"
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)

    def run_replay(self, binary, sample=None, scenario=None, report_dir=None):
        command = self.build_command(binary, sample, scenario, report_dir)
        # The replayer writes the report at the end, so a run that dies early would leave
        # the previous run's report in place and do_verify would judge that instead.
        report_path = os.path.join(report_dir or self.report_dir, self.REPORT_FILE)
        if os.path.isfile(report_path):
            os.remove(report_path)
        tdLog.info(f"taosreplay {command}")
        completed = subprocess.run(  # noqa: S603 - fixed binary, argv list, no shell
            [binary, *shlex.split(command)],
            capture_output=True,
            text=True,
            timeout=self.REPLAY_TIMEOUT_SEC,
            check=False,
        )
        tdLog.info(f"replay exit code: {completed.returncode}")
        for line in (completed.stderr or "").splitlines()[-20:]:
            tdLog.info(line)
        tdLog.info("replay finished")
        # 0 is a clean finish, 4 is "completed with errors or skips" and is judged by the
        # report. Anything else (2 input, 3 credentials/connection, 5 internal) means the
        # replay never ran, so the report would be absent or meaningless.
        assert completed.returncode in (0, 4), (
            f"taosreplay exited with {completed.returncode}; see the log above"
        )
        return completed

    # -------------------------------------------------------------------- impl
    def run_bootstrap(self):
        """Replay the prerequisite scenario so the main recording finds its schema.

        A recording captured from a long-lived instance queries databases and tables
        that were created before it started, so it cannot recreate its own schema.
        The scenario's ``bootstrap`` names another scenario whose recording carries
        those statements; replaying it first turns an empty cluster into the state
        the main recording was captured against.
        """
        name = self.scenario.get("bootstrap", "")
        if not name:
            return
        catalog = self.load_catalog()
        assert name in catalog, f"scenario {self.scenario_name}: unknown bootstrap scenario {name}"
        scenario = catalog[name]
        sample = self.sample_path(name, scenario)
        assert sample, f"scenario {self.scenario_name}: bootstrap recording {name} is not provisioned"
        report_dir = os.path.join(os.path.dirname(self.report_dir), name)
        os.makedirs(report_dir, exist_ok=True)
        tdLog.info(f"scenario {self.scenario_name}: replaying bootstrap scenario {name} first")
        self.run_replay(self.require_replayer(), sample, scenario, report_dir)
        report = self.read_report(report_dir)
        executed = int(report.get("callsExecuted", 0))
        tdLog.info(
            f"bootstrap {name}: executed={executed} skipped={report.get('callsSkipped')} "
            f"countMismatches={report.get('countMismatches')}"
        )
        assert executed > 0, f"bootstrap replay {name} executed no call"

    def do_replay(self):
        self.run_replay(self.require_replayer())

    def do_verify(self):
        report = self.read_report()
        executed = int(report.get("callsExecuted", 0))
        skipped = int(report.get("callsSkipped", 0))
        api_errors = int(report.get("apiErrors", 0))
        count_mismatches = int(report.get("countMismatches", 0))

        total = executed + skipped
        assert total > 0, "replay reported no calls at all"
        ratio = executed / total
        error_ratio = api_errors / total
        mismatch_ratio = count_mismatches / total

        tdLog.info(
            f"executed={executed} skipped={skipped} apiErrors={api_errors} "
            f"countMismatches={count_mismatches} executedRatio={ratio:.3f} "
            f"countMismatchRatio={mismatch_ratio:.4f}"
        )

        assert executed > 0, "replay executed no call; check the target is reachable"
        assert ratio >= self.scenario["minExecutedRatio"], (
            f"executed ratio {ratio:.3f} below {self.scenario['minExecutedRatio']}"
        )
        # A recording replayed into a rebuilt cluster legitimately returns different row
        # counts on a stable, reproducible subset, so scenarios may express the budget as
        # a fraction of all calls instead of an absolute count.
        limit_ratio = self.scenario.get("maxCountMismatchRatio")
        if limit_ratio is not None:
            assert mismatch_ratio <= limit_ratio, (
                f"row-count mismatch ratio {mismatch_ratio:.4f} above {limit_ratio}"
            )
        limit_count = self.scenario.get("maxCountMismatches")
        if limit_count is not None:
            assert count_mismatches <= limit_count, (
                f"row-count mismatches {count_mismatches} above {limit_count}"
            )
        assert error_ratio <= self.scenario["maxApiErrorRatio"], (
            f"api error ratio {error_ratio:.3f} above {self.scenario['maxApiErrorRatio']}"
        )
        print(f"scenario {self.scenario_name}: replay report accepted [passed]")

    # -------------------------------------------------------------------- main
    def test_replay_recorded_workload(self):
        """Tools: replay a recorded workload

        1. Replay the scenario's bootstrap recording when the scenario declares one,
           so a recording captured from a provisioned cluster finds its schema
        2. Replay a provisioned .wlr recording against the local cluster
        3. Accept the report when the executed ratio, row-count mismatches and
           api error ratio stay within the scenario thresholds

        Catalog:
            - Tools

        Since: v3.4.3.9

        Labels: common,replay,functional

        Jira: None

        History:
            - 2026-11-07 Alex Duan created
        """
        # Resolve the main recording first: run_bootstrap() mutates the cluster, so a
        # missing sample must fail before any state is created, not after.
        self.require_sample()
        self.run_bootstrap()
        self.do_replay()
        self.do_verify()
