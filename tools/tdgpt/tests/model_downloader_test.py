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

import os
import runpy
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

tdgpt_root = os.path.dirname(os.path.abspath(__file__)) + "/.."
sys.path.append(tdgpt_root)
sys.path.append(os.path.join(tdgpt_root, "taosanalytics", "misc"))

tsfmservice_dir = os.path.join(tdgpt_root, "taosanalytics", "tsfmservice")


def test_model_downloader_invalid_bool_exits(monkeypatch, capsys):
    monkeypatch.setitem(
        sys.modules,
        "tqdm",
        SimpleNamespace(tqdm=lambda items: items),
    )
    monkeypatch.setitem(
        sys.modules,
        "huggingface_hub",
        SimpleNamespace(snapshot_download=mock.Mock()),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["model_downloader.py", "model_dir", "model_name", "notabool"],
    )

    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module("taosanalytics.misc.model_downloader", run_name="__main__")

    assert excinfo.value.code == 1
    assert "notabool" in capsys.readouterr().out


def test_tsfmservice_download_helper_path_has_import_precedence():
    for server in [
        "chronos-server.py",
        "timemoe-server.py",
        "timesfm-server.py",
        "moirai-server.py",
        "moment-server.py",
    ]:
        with open(os.path.join(tsfmservice_dir, server), encoding="utf-8") as handle:
            source = handle.read()
            assert (
                'sys.path.append(os.path.join(os.path.dirname(__file__), "..", "misc"))'
                not in source
            )
            assert (
                'sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "misc"))'
                in source
            )
