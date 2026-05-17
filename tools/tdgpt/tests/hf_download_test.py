"""
Tests for hf_download helpers.
"""
import importlib
import os
import sys
from types import SimpleNamespace
from unittest import mock


sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")


def test_snapshot_download_with_fallback_accepts_positional_args(monkeypatch):
    module_name = "taosanalytics.misc.hf_download"
    fake_hf = SimpleNamespace(snapshot_download=mock.Mock(return_value="ok"))
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)
    if module_name in sys.modules:
        del sys.modules[module_name]
    hf_download = importlib.import_module(module_name)

    result = hf_download.snapshot_download_with_fallback("repo-id", "local-dir")

    assert result == "ok"
    fake_hf.snapshot_download.assert_called_once_with(
        repo_id="repo-id",
        local_dir="local-dir",
        endpoint=None,
    )
