"""
Tests for hf_download helpers.
"""
import importlib
import os
import sys
from types import SimpleNamespace
from unittest import mock

import pytest


sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")


def import_hf_download(monkeypatch):
    module_name = "taosanalytics.misc.hf_download"
    fake_hf = SimpleNamespace(snapshot_download=mock.Mock())
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)
    if module_name in sys.modules:
        del sys.modules[module_name]
    hf_download = importlib.import_module(module_name)
    return hf_download, fake_hf.snapshot_download


def test_parse_bool_false_string(monkeypatch):
    hf_download, _ = import_hf_download(monkeypatch)

    assert hf_download.parse_bool("False") is False


def test_resolve_endpoint_false_string_returns_none(monkeypatch):
    hf_download, _ = import_hf_download(monkeypatch)
    monkeypatch.delenv("TAOS_HF_ENDPOINT", raising=False)
    monkeypatch.delenv("HF_ENDPOINT", raising=False)

    assert hf_download.resolve_endpoint("False") is None


def test_resolve_endpoint_env_precedence(monkeypatch):
    hf_download, _ = import_hf_download(monkeypatch)
    monkeypatch.setenv("TAOS_HF_ENDPOINT", "https://taos.example")
    monkeypatch.setenv("HF_ENDPOINT", "https://hf.example")

    assert hf_download.resolve_endpoint(True) == "https://taos.example"

    monkeypatch.delenv("TAOS_HF_ENDPOINT")
    assert hf_download.resolve_endpoint(True) == "https://hf.example"

    monkeypatch.delenv("HF_ENDPOINT")
    assert hf_download.resolve_endpoint(True) == hf_download.DEFAULT_HF_MIRROR_ENDPOINT


def test_snapshot_download_with_fallback_accepts_positional_enable(monkeypatch):
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.delenv("TAOS_HF_ENDPOINT", raising=False)
    monkeypatch.delenv("HF_ENDPOINT", raising=False)
    snapshot_download.return_value = "ok"

    result = hf_download.snapshot_download_with_fallback("repo-id", "local-dir", True)

    assert result == "ok"
    snapshot_download.assert_called_once_with(
        repo_id="repo-id",
        local_dir="local-dir",
        endpoint=hf_download.DEFAULT_HF_MIRROR_ENDPOINT,
    )


def test_snapshot_download_with_fallback_calls_official_on_failure(monkeypatch):
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.setenv("TAOS_HF_ENDPOINT", "https://custom.endpoint")

    def side_effect(**kwargs):
        if kwargs.get("endpoint") == "https://custom.endpoint":
            raise RuntimeError("boom")
        return "ok"

    snapshot_download.side_effect = side_effect

    result = hf_download.snapshot_download_with_fallback("repo-id", "local-dir", True)

    assert result == "ok"
    assert snapshot_download.call_args_list == [
        mock.call(
            repo_id="repo-id",
            local_dir="local-dir",
            endpoint="https://custom.endpoint",
        ),
        mock.call(
            repo_id="repo-id",
            local_dir="local-dir",
            endpoint=hf_download.OFFICIAL_HF_ENDPOINT,
        ),
    ]


def test_snapshot_download_with_fallback_uses_official_when_env_endpoint_fails(monkeypatch):
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.delenv("TAOS_HF_ENDPOINT", raising=False)
    monkeypatch.setenv("HF_ENDPOINT", "https://custom.endpoint")
    snapshot_download.side_effect = [RuntimeError("boom"), "ok"]

    result = hf_download.snapshot_download_with_fallback("repo-id", "local-dir", False)

    assert result == "ok"
    assert snapshot_download.call_args_list == [
        mock.call(
            repo_id="repo-id",
            local_dir="local-dir",
            endpoint=None,
        ),
        mock.call(
            repo_id="repo-id",
            local_dir="local-dir",
            endpoint=hf_download.OFFICIAL_HF_ENDPOINT,
        ),
    ]


def test_snapshot_download_with_fallback_ignores_endpoint_kwarg(monkeypatch):
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.delenv("TAOS_HF_ENDPOINT", raising=False)
    monkeypatch.delenv("HF_ENDPOINT", raising=False)
    snapshot_download.return_value = "ok"

    result = hf_download.snapshot_download_with_fallback(
        "repo-id",
        "local-dir",
        True,
        endpoint="https://ignored.endpoint",
    )

    assert result == "ok"
    snapshot_download.assert_called_once_with(
        repo_id="repo-id",
        local_dir="local-dir",
        endpoint=hf_download.DEFAULT_HF_MIRROR_ENDPOINT,
    )


def test_snapshot_download_with_fallback_keeps_default_download_quiet(monkeypatch, capsys):
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.delenv("TAOS_HF_ENDPOINT", raising=False)
    monkeypatch.delenv("HF_ENDPOINT", raising=False)
    snapshot_download.return_value = "ok"

    result = hf_download.snapshot_download_with_fallback("repo-id", "local-dir", False)

    assert result == "ok"
    assert capsys.readouterr().out == ""


def test_snapshot_download_with_fallback_reraises_official_env_endpoint(monkeypatch):
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.delenv("TAOS_HF_ENDPOINT", raising=False)
    monkeypatch.setenv("HF_ENDPOINT", hf_download.OFFICIAL_HF_ENDPOINT)
    snapshot_download.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        hf_download.snapshot_download_with_fallback("repo-id", "local-dir", True)

    snapshot_download.assert_called_once_with(
        repo_id="repo-id",
        local_dir="local-dir",
        endpoint=hf_download.OFFICIAL_HF_ENDPOINT,
    )


def test_snapshot_download_with_fallback_reraises_official(monkeypatch):
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    snapshot_download.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        hf_download.snapshot_download_with_fallback("repo-id", "local-dir", False)

    snapshot_download.assert_called_once_with(
        repo_id="repo-id",
        local_dir="local-dir",
        endpoint=None,
    )


def test_permission_error_reraises_without_fallback(monkeypatch):
    """Local PermissionError must not trigger fallback to official endpoint."""
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.setenv("TAOS_HF_ENDPOINT", "https://custom.endpoint")
    snapshot_download.side_effect = PermissionError("Permission denied")

    with pytest.raises(PermissionError):
        hf_download.snapshot_download_with_fallback("repo-id", "local-dir", True)

    snapshot_download.assert_called_once()


def test_http_404_reraises_without_fallback(monkeypatch):
    """HTTP 404 must not trigger fallback — the repo doesn't exist on any endpoint."""
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.setenv("TAOS_HF_ENDPOINT", "https://custom.endpoint")

    response = mock.Mock()
    response.status_code = 404
    exc = OSError("Not Found")
    exc.response = response
    snapshot_download.side_effect = exc

    with pytest.raises(OSError):
        hf_download.snapshot_download_with_fallback("repo-id", "local-dir", True)

    snapshot_download.assert_called_once()


def test_connection_error_triggers_fallback(monkeypatch):
    """Network ConnectionError on mirror should fall back to official endpoint."""
    hf_download, snapshot_download = import_hf_download(monkeypatch)
    monkeypatch.setenv("TAOS_HF_ENDPOINT", "https://custom.endpoint")

    def side_effect(**kwargs):
        if kwargs.get("endpoint") == "https://custom.endpoint":
            raise ConnectionError("Connection refused")
        return "ok"

    snapshot_download.side_effect = side_effect

    result = hf_download.snapshot_download_with_fallback("repo-id", "local-dir", True)

    assert result == "ok"
    assert snapshot_download.call_count == 2
