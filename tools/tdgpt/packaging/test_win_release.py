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
import os
import shutil
import subprocess
import sys
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("win_release.py")
SPEC = importlib.util.spec_from_file_location("tdgpt_win_release", MODULE_PATH)
win_release = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(win_release)


def test_windows_package_version_with_copyright_header(tmp_path, monkeypatch):
    source_dir = tmp_path / "source"
    analytics = source_dir / "taosanalytics"
    analytics.mkdir(parents=True)
    (analytics / "__init__.py").write_text(
        "# Copyright (c) TAOS Data\n\n__version__ = '3.3.7.1'\n"
        "raise RuntimeError('version lookup must not import analytics')\n",
        encoding="utf-8",
    )
    install_dir = tmp_path / "Windows package"
    monkeypatch.setattr(win_release.install_info, "source_dir", str(source_dir))
    monkeypatch.setattr(win_release.install_info, "install_dir", str(install_dir))
    monkeypatch.setattr(win_release.tdgpt_version, "version", "9.8.7")
    win_release.copy_python_files()

    bin_dir = install_dir / "bin"
    bin_dir.mkdir()
    ctl = bin_dir / "taosanodectl.py"
    shutil.copyfile(MODULE_PATH.parents[1] / "script" / ctl.name, ctl)
    for flag in ("-V", "--version"):
        env = dict(os.environ, PYTHONIOENCODING="gbk:strict")
        result = subprocess.run(
            [sys.executable, "-S", str(ctl), flag],
            capture_output=True, text=True, encoding="gbk", env=env,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == (
            "TDgpt - TDengine TSDB(c) Time-Series Data Analytics Platform (ver 9.8.7)"
        )


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


def test_lite_installer_preserves_dynamic_resource_directories():
    template_path = MODULE_PATH.parent / "installer" / "tdgpt-lite.iss"
    installer_template = template_path.read_text(encoding="utf-8-sig")
    uninstall_script = (MODULE_PATH.parent.parent / "script" / "uninstall.py").read_text(
        encoding="utf-8"
    )

    for directory in ("model", "model_venvs", "venvs"):
        assert f'Name: "{{app}}\\{directory}"; Permissions: everyone-modify; Flags: uninsneveruninstall' in installer_template
    assert 'Name: "{app}\\data\\resource"; Permissions: everyone-modify; Flags: uninsneveruninstall' in installer_template

    default_directories = uninstall_script.split("directories = [", 1)[1].split(
        "if self.remove_venv:", 1
    )[0]
    assert 'self.install_dir / "data"' not in default_directories
    remove_data_block = uninstall_script.split("if self.remove_data:", 1)[1].split(
        "if self.remove_model:", 1
    )[0]
    assert 'self.install_dir / "data"' in remove_data_block


def test_installers_use_installed_icon_for_windows_apps_settings():
    for template_name in ("tdgpt.iss", "tdgpt-lite.iss"):
        template_path = MODULE_PATH.parent / "installer" / template_name
        installer_template = template_path.read_text(encoding="utf-8-sig")

        assert "UninstallDisplayIcon={app}\\favicon.ico" in installer_template
        assert (
            'Source: "{#MyAppIco}"; DestDir: "{app}"; '
            'DestName: "favicon.ico"; Flags: ignoreversion'
        ) in installer_template


def test_installers_do_not_package_obsolete_sample_sql_files():
    for template_name in ("tdgpt.iss", "tdgpt-lite.iss"):
        template_path = MODULE_PATH.parent / "installer" / template_name
        installer_template = template_path.read_text(encoding="utf-8-sig")

        assert 'Source: "{#MyAppSourceDir}\\resource\\*"' not in installer_template
        assert 'Name: "{app}\\resource"; Type: filesandordirs' in installer_template


def test_lite_installer_removes_legacy_shortcuts_without_publishing_new_ones():
    template_path = MODULE_PATH.parent / "installer" / "tdgpt-lite.iss"
    installer_template = template_path.read_text(encoding="utf-8-sig")

    assert "[Icons]" not in installer_template
    assert "[InstallDelete]" in installer_template
    assert '#define MyLegacyAppName "{{LEGACY_APP_NAME}}"' in installer_template
    assert (
        'Name: "{commonprograms}\\{#MyAppName}"; Type: filesandordirs'
        in installer_template
    )
    assert (
        'Name: "{commonprograms}\\{#MyLegacyAppName}"; Type: filesandordirs'
        in installer_template
    )
    assert (
        'Name: "{commondesktop}\\Start Taosanode.lnk"; Type: files'
        in installer_template
    )


def test_generated_lite_installer_names_current_and_legacy_groups(tmp_path, monkeypatch):
    monkeypatch.setattr(win_release.install_info, "release_dir", str(tmp_path))
    monkeypatch.setattr(win_release.install_info, "install_dir", str(tmp_path / "install"))
    monkeypatch.setattr(win_release.install_info, "source_dir", str(tmp_path / "install"))
    monkeypatch.setattr(win_release.install_info, "package_name", "tdgpt-lite-test")
    monkeypatch.setattr(win_release.install_info, "app_name", "TDengine TDgpt-OSS Lite")
    monkeypatch.setattr(win_release.install_info, "legacy_app_name", "TDengine TDgpt-OSS")
    monkeypatch.setattr(win_release.install_info, "product_full_name", "TDgpt Lite Test")
    monkeypatch.setattr(win_release.install_info, "resource_package_url", "")
    monkeypatch.setattr(win_release.install_info, "lite", True)
    monkeypatch.setattr(win_release.tdgpt_version, "version", "1.0.0")

    generated_iss = Path(win_release.create_iss_script()).read_text(encoding="utf-8")

    assert '#define MyAppName "TDengine TDgpt-OSS Lite"' in generated_iss
    assert '#define MyLegacyAppName "TDengine TDgpt-OSS"' in generated_iss
    assert "{{LEGACY_APP_NAME}}" not in generated_iss
    assert "{{APP_NAME}}" not in generated_iss
    assert "{{VERSION}}" not in generated_iss
    assert "WizardStyle=modern" in generated_iss
    assert "WizardForm.Caption := '{#MyAppName} {#MyAppVersion} Setup';" in generated_iss
    assert "UninstallAppFullTitle={#MyAppName} {#MyAppVersion} Uninstall" in generated_iss
    assert "ConfirmUninstall=Are you sure you want to uninstall {#MyAppName}?" in generated_iss


def test_lite_installer_aligns_completion_text_and_log_checkbox():
    installer_template = (MODULE_PATH.parent / "installer" / "tdgpt-lite.iss").read_text(
        encoding="utf-8-sig"
    )

    for control in (
        "WizardForm.PageNameLabel",
        "WizardForm.PageDescriptionLabel",
        "WizardForm.InnerNotebook",
        "WizardForm.FinishedHeadingLabel",
        "WizardForm.FinishedLabel",
        "FinishNotesLabel",
        "WizardForm.RunList",
    ):
        assert f"MoveControlToClientX({control}, CaptionX);" in installer_template
    assert "WizardForm.AdjustLabelHeight(FinishNotesLabel);" in installer_template
    assert "WizardForm.RunList.Top := FinishNotesLabel.Top + FinishNotesLabel.Height" in installer_template
    assert "if WizardForm.YesRadio.Visible then\n      exit;" in installer_template
    assert 'Description: "View the installation log";' in installer_template


def test_lite_uninstall_updates_status_without_changing_retention():
    installer_template = (MODULE_PATH.parent / "installer" / "tdgpt-lite.iss").read_text(
        encoding="utf-8-sig"
    )

    uninstall_run = installer_template.split("[UninstallRun]", 1)[1].split("[", 1)[0]
    assert 'Parameters: "/C call ""{app}\\uninstall.bat""";' in uninstall_run
    # These entry callbacks run while Setup records the uninstall command,
    # when UninstallProgressForm does not exist, including in silent mode.
    assert "BeforeInstall:" not in uninstall_run
    assert "AfterInstall:" not in uninstall_run
    assert "procedure InitializeUninstallProgressForm;" in installer_template
    uninstall_event = installer_template.split(
        "procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);", 1
    )[1]
    before_removal = uninstall_event.split("CurUninstallStep = usPostUninstall", 1)[0]
    assert "if CurUninstallStep = usUninstall then" in before_removal
    assert (
        "UninstallProgressForm.StatusLabel.Caption := "
        "'Stopping services and removing application files...';"
    ) in before_removal
    assert "UninstallProgressForm.Refresh;" in before_removal
    assert "IconFile := ExpandConstant('{app}\\favicon.ico');" in installer_template
    assert "DelTree(" not in installer_template
    assert "[UninstallDelete]" not in installer_template
    for directory in ("cfg", "data", "log", "model", "model_venvs", "venvs"):
        assert f'Name: "{{app}}\\{directory}"; Permissions: everyone-modify; Flags: uninsneveruninstall' in installer_template


def test_silent_installer_does_not_prompt_for_prerequisite_or_existing_install():
    template_path = MODULE_PATH.parent / "installer" / "tdgpt.iss"
    installer_template = template_path.read_text(encoding="utf-8-sig")

    assert "Silent installation accepted existing install directory" in installer_template
    assert "if not WizardSilent() then\n      MsgBox(MessageText, mbCriticalError, MB_OK);" in installer_template
