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

import logging
import sys
from types import SimpleNamespace

from taosanalytics.tsfm import foundation_util


def test_get_taosanodectl_cmd_finds_windows_install_layout(tmp_path, monkeypatch):
    install_root = tmp_path / "taosanode"
    module_path = install_root / "lib" / "taosanalytics" / "tsfm" / "foundation_util.py"
    script_path = install_root / "bin" / "taosanodectl.py"
    module_path.parent.mkdir(parents=True)
    script_path.parent.mkdir(parents=True)
    module_path.touch()
    script_path.touch()

    # The layout is resolved from the module that owns the lookup: patching
    # foundation_model.__file__ would not move foundation_util's own path.
    monkeypatch.setattr(foundation_util, "__file__", str(module_path))
    monkeypatch.setattr("shutil.which", lambda _command: None)
    # The forwarded level comes from configuration; pin it so this test stays
    # about the layout discovery it is named after.
    monkeypatch.setattr(foundation_util, "taosanodectl_log_level", lambda: "debug")

    assert foundation_util.get_taosanodectl_cmd() == [
        sys.executable,
        str(script_path),
        "--log-level",
        "debug",
    ]


def _configure_monkeypatch(monkeypatch, conf):
    monkeypatch.setattr(
        foundation_util,
        "Configure",
        SimpleNamespace(get_instance=lambda: SimpleNamespace(_conf=conf)),
    )


def test_taosanodectl_log_level_follows_configured_level(monkeypatch):
    _configure_monkeypatch(monkeypatch, {"log_level": logging.DEBUG})
    assert foundation_util.taosanodectl_log_level() == "debug"

    _configure_monkeypatch(monkeypatch, {"log_level": logging.WARNING})
    assert foundation_util.taosanodectl_log_level() == "warning"

    # A non-standard level is forwarded numerically: the child accepts numbers.
    _configure_monkeypatch(monkeypatch, {"log_level": 15})
    assert foundation_util.taosanodectl_log_level() == "15"


def test_taosanodectl_log_level_defaults_to_debug(monkeypatch):
    _configure_monkeypatch(monkeypatch, {})
    assert foundation_util.taosanodectl_log_level() == "debug"
