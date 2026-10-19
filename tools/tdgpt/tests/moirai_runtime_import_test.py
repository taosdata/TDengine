# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Real-runtime regression for Moirai's native dependency initialization.

Set TDGPT_TSFM_PYTHON to the TSFM runtime interpreter. The Lite interpreter
does not include these dependencies. Run each check in a fresh process so
previous imports cannot hide DLL initialization failures.
"""

import os
import subprocess
import unittest
from pathlib import Path


@unittest.skipUnless(os.environ.get("TDGPT_TSFM_PYTHON"), "requires TSFM runtime")
class MoiraiRuntimeImportTest(unittest.TestCase):
    def test_server_imports_in_fresh_process(self):
        script = Path(os.environ.get(
            "TDGPT_MOIRAI_SERVER",
            Path(__file__).resolve().parents[1] / "taosanalytics" /
            "tsfmservice" / "moirai-server.py",
        ))
        for attempt in range(3):
            with self.subTest(attempt=attempt):
                result = subprocess.run(
                    [os.environ["TDGPT_TSFM_PYTHON"], str(script), "--help"],
                    capture_output=True, text=True, errors="replace", timeout=120,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("Moirai forecast model server", result.stdout)


if __name__ == "__main__":
    unittest.main()
