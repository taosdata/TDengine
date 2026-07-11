import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("win_release.py")
SPEC = importlib.util.spec_from_file_location("tdgpt_win_release", MODULE_PATH)
win_release = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(win_release)


def test_copy_icon_file_uses_gitlab_monorepo_taos_internal(tmp_path):
    source_dir = tmp_path / "source" / "taos-community" / "tools" / "tdgpt"
    install_dir = tmp_path / "release" / "install"
    icon_source = tmp_path / "source" / "taos-internal" / "packaging" / "windows" / "favicon.ico"

    icon_source.parent.mkdir(parents=True)
    source_dir.mkdir(parents=True)
    install_dir.mkdir(parents=True)
    icon_source.write_bytes(b"ico")

    win_release.install_info.source_dir = str(source_dir)
    win_release.install_info.install_dir = str(install_dir)

    icon_name = win_release.copy_icon_file()

    assert icon_name == "favicon.ico"
    assert (install_dir / "favicon.ico").read_bytes() == b"ico"


def test_copy_enterprise_files_uses_gitlab_monorepo_taos_internal(tmp_path):
    source_dir = tmp_path / "source" / "taos-community" / "tools" / "tdgpt"
    install_dir = tmp_path / "release" / "install"
    misc_source = (
        tmp_path
        / "source"
        / "taos-internal"
        / "source"
        / "kit"
        / "tools"
        / "tdgpt"
        / "taosanalytics"
        / "misc"
    )
    misc_dest = install_dir / "lib" / "taosanalytics" / "misc"

    misc_source.mkdir(parents=True)
    misc_dest.mkdir(parents=True)
    source_dir.mkdir(parents=True)
    (misc_source / "enterprise_only.py").write_text("x = 1\n", encoding="utf-8")

    win_release.install_info.source_dir = str(source_dir)
    win_release.install_info.install_dir = str(install_dir)
    win_release.tdgpt_version.ver_type = "enterprise"

    win_release.copy_enterprise_files()

    assert (misc_dest / "enterprise_only.py").read_text(encoding="utf-8") == "x = 1\n"
