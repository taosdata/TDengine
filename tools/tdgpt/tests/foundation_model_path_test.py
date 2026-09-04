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

    assert foundation_model._get_taosanodectl_cmd() == [
        foundation_model.sys.executable,
        str(script_path),
    ]
