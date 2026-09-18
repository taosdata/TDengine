import logging
from types import SimpleNamespace

from taosanalytics.tsfmservice import foundation_model


def test_get_taosanodectl_cmd_finds_windows_install_layout(tmp_path, monkeypatch):
    install_root = tmp_path / "taosanode"
    module_path = install_root / "lib" / "taosanalytics" / "tsfmservice" / "foundation_model.py"
    script_path = install_root / "bin" / "taosanodectl.py"
    module_path.parent.mkdir(parents=True)
    script_path.parent.mkdir(parents=True)
    module_path.touch()
    script_path.touch()

    monkeypatch.setattr(foundation_model, "__file__", str(module_path))
    monkeypatch.setattr("shutil.which", lambda _command: None)
    # The forwarded level comes from configuration; pin it so this test stays
    # about the layout discovery it is named after.
    monkeypatch.setattr(foundation_model, "_taosanodectl_log_level", lambda: "debug")

    assert foundation_model._get_taosanodectl_cmd() == [
        foundation_model.sys.executable,
        str(script_path),
        "--log-level",
        "debug",
    ]


def _configure_monkeypatch(monkeypatch, conf):
    monkeypatch.setattr(
        foundation_model,
        "Configure",
        SimpleNamespace(get_instance=lambda: SimpleNamespace(_conf=conf)),
    )


def test_taosanodectl_log_level_follows_configured_level(monkeypatch):
    _configure_monkeypatch(monkeypatch, {"log_level": logging.DEBUG})
    assert foundation_model._taosanodectl_log_level() == "debug"

    _configure_monkeypatch(monkeypatch, {"log_level": logging.WARNING})
    assert foundation_model._taosanodectl_log_level() == "warning"

    # A non-standard level is forwarded numerically: the child accepts numbers.
    _configure_monkeypatch(monkeypatch, {"log_level": 15})
    assert foundation_model._taosanodectl_log_level() == "15"


def test_taosanodectl_log_level_defaults_to_debug(monkeypatch):
    _configure_monkeypatch(monkeypatch, {})
    assert foundation_model._taosanodectl_log_level() == "debug"
