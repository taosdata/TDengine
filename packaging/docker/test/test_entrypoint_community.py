#!/usr/bin/env python3
# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Static guard rails for packaging/docker/bin/entrypoint.sh.

Behaviour is covered by run-local.sh (needs docker). These checks run in CI,
where only the script text is available, and fail if the supervision contract
is broken again -- for example by reintroducing a command that the runtime
image does not ship.
"""

import re
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "bin" / "entrypoint.sh"


def code_lines():
    """Yield the script lines with comments and here-doc payloads removed."""
    text = SCRIPT.read_text(encoding="utf-8")
    out = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        out.append(line.split(" #", 1)[0] if " #" in line else line)
    return out


class EntrypointSupervisionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = SCRIPT.read_text(encoding="utf-8")
        cls.code = "\n".join(code_lines())

    def test_script_exists(self):
        self.assertTrue(SCRIPT.is_file(), f"missing {SCRIPT}")

    def test_no_procps_commands(self):
        """pkill/pgrep come from procps, which Dockerfile.base does not install."""
        for cmd in ("pkill", "pgrep"):
            self.assertNotRegex(
                self.code, rf"(^|[\s;&|(]){cmd}\s",
                f"'{cmd}' is not available in the image; track PIDs with '$!' instead")

    def test_tracks_child_pids(self):
        """Each background child must have its PID recorded."""
        for var in ("PID_TAOSD", "PID_TAOSADAPTER", "PID_TAOSKEEPER", "PID_TAOS_EXPLORER"):
            self.assertRegex(self.code, rf"{var}=\$!",
                             f"{var} is never captured from '$!'")

    def test_waits_for_first_child_to_exit(self):
        """A bare wait blocks until every child exits, which is the original bug."""
        self.assertIn("wait -n", self.code,
                      "must use 'wait -n' so a single dead child stops the container")

    def test_signal_handler_shuts_children_down(self):
        self.assertIn("cleanup_children", self.code)
        self.assertRegex(self.code, r"trap .*cleanup_children.*wait.*SIGTERM",
                         "the signal handler must signal the children and then wait")

    def test_readiness_loop_is_bounded(self):
        """'while true' without a bound keeps a half-dead container alive forever."""
        self.assertGreaterEqual(self.code.count("while true; do"), 2)
        self.assertRegex(self.code, r"ready_wait.*-gt \d+",
                         "readiness loops must abort after a bounded number of attempts")

    def test_check_command_failure_is_tolerated(self):
        """'taos --check' fails while taosd boots; under 'set -e' that aborted immediately."""
        self.assertRegex(self.code, r"es=\$\(.*\|\| true\)",
                         "'taos --check' output assignment must ignore its exit status")

    def test_bash_version_guard(self):
        self.assertIn("BASH_VERSINFO", self.code,
                      "'wait -n' needs bash >= 4.3; guard so a base-image change fails loudly")


if __name__ == "__main__":
    unittest.main(verbosity=2)
