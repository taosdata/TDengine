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

import importlib.util
from pathlib import Path


def _load_log_reader():
    module_path = (
        Path(__file__).resolve().parents[1] / "utils" / "log_reader.py"
    )
    spec = importlib.util.spec_from_file_location("log_reader", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.LogReader


class TestLogReader:
    def test_reads_appended_complete_lines(self, tmp_path):
        log_path = tmp_path / "taosdlog.0"
        log_path.write_bytes(b"existing line\n")
        reader = _load_log_reader()([str(log_path)])

        with log_path.open("ab") as log:
            log.write(b"new line\n")

        assert reader.read() == "new line\n"

    def test_detects_truncate_then_rewrite_past_old_offset(
        self, tmp_path
    ):
        log_path = tmp_path / "taosdlog.0"
        old_content = b"old line one\nold line two\n"
        log_path.write_bytes(old_content)
        reader = _load_log_reader()([str(log_path)])

        replacement = b"replacement line one\nreplacement line two\n"
        log_path.write_bytes(replacement)

        assert len(replacement) > len(old_content)
        assert reader.read() == replacement.decode()
