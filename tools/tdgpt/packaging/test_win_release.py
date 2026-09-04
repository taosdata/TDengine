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
    icon_source = (
        tmp_path / "source" / "taos-internal" / "packaging" / "windows" / "favicon.ico"
    )

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


def test_create_install_script_prefers_external_python_runtime(tmp_path):
    install_dir = tmp_path / "release" / "install"
    install_dir.mkdir(parents=True)

    win_release.install_info.install_dir = str(install_dir)
    win_release.install_info.app_name = "TDengine TDgpt-Enterprise"
    win_release.install_info.all_models = False

    win_release.create_install_script()

    install_bat = (install_dir / "install.bat").read_text(encoding="utf-8")
    assert 'call :parse_runtime_arg %*' in install_bat
    assert 'if defined PYTHON_RUNTIME_DIR (' in install_bat
    assert 'set "TDGPT_PYTHON_RUNTIME_DIR=%PYTHON_RUNTIME_DIR%"' in install_bat
    assert 'if not exist "%PYTHON_RUNTIME_DIR%\\python.exe" (' in install_bat


def test_lite_installer_skips_model_wizard_and_forces_no_model_source():
    template_path = MODULE_PATH.parent / "installer" / "tdgpt.iss"
    installer_template = template_path.read_text(encoding="utf-8-sig")

    assert "if IsLiteBuild and (" in installer_template
    assert "(PageID = InstallModePage.ID)" in installer_template
    assert "(PageID = ModelSourcePage.ID)" in installer_template
    assert "if IsLiteBuild then\n    begin\n      UseResourcePackage := False;" in installer_template
    assert "ModelSource := 'none';" in installer_template
    assert "SetErrorFlag" not in installer_template
    assert "MsgBox(MessageText, mbCriticalError, MB_OK);\n  ExitProcess(1);" in installer_template


def test_lite_installer_waits_for_existing_service_and_file_unlocks():
    template_path = MODULE_PATH.parent / "installer" / "tdgpt-lite.iss"
    installer_template = template_path.read_text(encoding="utf-8-sig")

    assert "function WaitForServiceStop" in installer_template
    assert "StopExistingModelServices(TargetDir);" in installer_template
    assert "Exec(CandidatePath, '--version', TargetDir" in installer_template
    assert 'ServiceScript + \"\\\" stop\"' not in installer_template
    assert "KillProcessesFromPidDir" in installer_template
    assert "taskkill /F /T /IM taosanode-winsw.exe" in installer_template
    assert "WaitForFileUnlock(WinSWPath, 15)" in installer_template
    assert "WaitForFileUnlock(RuntimePythonPath, 15)" in installer_template
    assert "WaitForFileUnlock(RuntimeDllPath, 15)" in installer_template
    assert "TDgpt Lite processes are still running:" in installer_template
    assert "PostInstallTimeoutMilliseconds" in installer_template
    assert "Installation timed out" in installer_template


def test_silent_installer_does_not_prompt_for_prerequisite_or_existing_install():
    template_path = MODULE_PATH.parent / "installer" / "tdgpt.iss"
    installer_template = template_path.read_text(encoding="utf-8-sig")

    assert "Silent installation accepted existing install directory" in installer_template
    assert "if not WizardSilent() then\n      MsgBox(MessageText, mbCriticalError, MB_OK);" in installer_template
