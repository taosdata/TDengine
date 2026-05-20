import os
import runpy
import sys
from types import SimpleNamespace
from unittest import mock

import pytest


tdgpt_root = os.path.dirname(os.path.abspath(__file__)) + "/.."
sys.path.append(tdgpt_root)
sys.path.append(os.path.join(tdgpt_root, "taosanalytics", "misc"))


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
