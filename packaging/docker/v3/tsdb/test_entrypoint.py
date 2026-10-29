# Copyright (c) TAOS Data, Inc. All rights reserved.
#
# SPDX-License-Identifier: LicenseRef-TAOSData-Commercial
#
# This file is proprietary to TAOS Data, Inc. and may contain confidential
# information. No rights are granted by this file. Use is permitted only
# under a written agreement with TAOS Data, Inc. or an authorized affiliate.
# Unauthorized copying, modification, distribution, or disclosure, in whole
# or in part, by any medium, is strictly prohibited.

"""Exercise the real entrypoint functions using isolated child processes, no Docker."""
import os
import platform
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


@unittest.skipUnless(platform.system() == "Linux", "Requires Linux procps and Bash")
class EntrypointTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        source = (HERE / "entrypoint.sh").read_text()
        definitions, invocation = source.rsplit('\nmain "$@"', 1)
        self.assertEqual(invocation.strip(), "")
        self.library = self.root / "functions.sh"
        self.library.write_text(definitions)

    def run_script(self, script, expected=0, send_signal=None):
        def reset_interrupt():
            # CI runners may ignore SIGINT. A fresh container entrypoint does not;
            # do not let the runner's inherited disposition change this fixture.
            signal.signal(signal.SIGINT, signal.SIG_DFL)

        process = subprocess.Popen(
            ["bash", "-c", 'source "$1"\n' + script, "entrypoint-test", str(self.library)],
            cwd=self.root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, start_new_session=True, preexec_fn=reset_interrupt,
        )
        try:
            if send_signal:
                import time
                deadline = time.monotonic() + 5
                while not (self.root / "ready").exists():
                    if process.poll() is not None or time.monotonic() > deadline:
                        self.fail("entrypoint did not become ready for signal test")
                    time.sleep(0.02)
                process.send_signal(send_signal)
            output, _ = process.communicate(timeout=12)
            self.assertEqual(process.returncode, expected, output)
            return output
        finally:
            # This process group belongs exclusively to this test, even on failure.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()

    def test_running_children_are_healthy(self):
        self.run_script('sleep 30 &\nPIDS="$!"\ncheck_managed_processes\ncleanup_children')

    def test_any_service_exit_is_failure_including_exit_zero(self):
        for status in (0, 7):
            with self.subTest(status=status):
                output = self.run_script(
                    '(exit ' + str(status) + ') &\nPIDS="$!"\n'
                    'while [ -e "/proc/$PIDS/status" ] && '
                    '[ "$(awk \'$1 == "State:" { print $2 }\' "/proc/$PIDS/status")" != Z ]; '
                    'do sleep 0.01; done\n'
                    'if check_managed_processes; then exit 98; fi\nPIDS=""\nexit 1', expected=1,
                )
                self.assertIn("Managed child process", output)

    def test_managed_zombie_is_unhealthy_even_when_kill_zero_succeeds(self):
        output = self.run_script(
            'mkdir -p fakeproc/4242\n'
            'printf "State:\\tZ (zombie)\\n" > fakeproc/4242/status\n'
            'kill() { return 0; }\n'
            'PIDS=4242\n'
            'if check_managed_processes "$PWD/fakeproc"; then exit 98; fi\n'
            'exit 0',
        )
        self.assertIn("Managed child process 4242 is a zombie", output)

    def test_startup_detects_a_service_that_already_exited(self):
        self.run_script('(exit 0) &\nPIDS="$!"\nsleep 0.1\n'
                        'wait_service_ready localhost 6030 1 && exit 98\nexit 1', expected=1)

    def test_exit_trap_preserves_failure_and_stops_child(self):
        output = self.run_script(
            "bash -c 'trap \"echo child-stopped; exit 0\" TERM; touch child-ready; "
            "while :; do sleep 0.05; done' &\nPIDS=\"$!\"\n"
            'while [ ! -f child-ready ]; do sleep 0.02; done\nexit 17', expected=17,
        )
        self.assertIn("child-stopped", output)

    def test_sigterm_and_sigint_are_clean(self):
        for sig in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=sig):
                (self.root / "ready").unlink(missing_ok=True)
                self.run_script('sleep 30 &\nPIDS="$!"\ntouch ready\n'
                                'while :; do sleep 0.1; done', send_signal=sig)

    def test_cleanup_allows_graceful_shutdown(self):
        output = self.run_script(
            "bash -c 'trap \"echo graceful; exit 0\" TERM; touch child-ready; "
            "while :; do sleep 0.05; done' &\nPIDS=\"$!\"\n"
            'while [ ! -f child-ready ]; do sleep 0.02; done\n'
            'cleanup_children\nwait $PIDS',
        )
        self.assertIn("graceful", output)
        self.assertNotIn("forcing shutdown", output)

    def test_cleanup_kills_stubborn_service_after_timeout(self):
        output = self.run_script(
            "bash -c 'trap \"\" TERM; touch child-ready; while :; do sleep 0.05; done' &\n"
            'PIDS="$!"\nSHUTDOWN_TIMEOUT=1\n'
            'while [ ! -f child-ready ]; do sleep 0.02; done\n'
            'cleanup_children\nwait $PIDS && exit 98\nexit 0',
        )
        self.assertIn("forcing shutdown", output)

    def test_finished_registration_helper_is_not_a_service_failure(self):
        self.run_script('sleep 30 &\nPIDS="$!"\n(exit 0) &\nhelper=$!\n'
                        'wait "$helper"\ncheck_managed_processes\ncleanup_children')

    def test_no_enabled_services_exits_nonzero(self):
        self.run_script('for name in init_env_vars read_taosd_config core_dump_config '
                        'detect_node_role detect_initialization_status show_startup_info '
                        'start_components; do eval "$name() { :; }"; done\n'
                        'DISABLE_TAOSD=1\nmain', expected=1)

    def test_custom_command_retains_its_exit_status(self):
        self.run_script("support_custom_commands bash -c 'exit 23'", expected=23)

    def test_xnode_lookup_preserves_long_urls(self):
        self.run_script('TAOS_ROOT_PASSWORD=taosdata\nFIRST_EP_HOST=localhost\nFIRST_EP_PORT=6030\n'
                        'url=tsdb-lifecycle-long-hostname-123456:6055\n'
                        'taos() {\n'
                        '  if [ "$1 $2" = "-w 1024" ]; then\n'
                        '    printf "1 | %s | online |\\n" "$url"\n'
                        '  else printf "1 | tsdb-lifecycle-long-hostnam... | online |\\n"; fi\n'
                        '}\nxnode_exists "$url"')

    def test_xnode_lookup_distinguishes_missing_from_query_error(self):
        self.run_script('TAOS_ROOT_PASSWORD=taosdata\nFIRST_EP_HOST=localhost\nFIRST_EP_PORT=6030\n'
                        'taos() { echo "Query OK, 0 row(s) in set"; }\n'
                        'rc=0\nxnode_exists localhost:6055 || rc=$?\n[ "$rc" = 1 ]\n'
                        'taos() { echo "DB error: unavailable"; }\n'
                        'rc=0\nxnode_exists localhost:6055 || rc=$?\n[ "$rc" = 2 ]')


if __name__ == "__main__":
    unittest.main()
