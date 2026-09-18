import importlib.util
import io
import platform
import sys
import tarfile
import zipfile
from pathlib import Path
from unittest import mock

import pytest


@pytest.fixture(scope="module")
def install_module():
    module_path = Path(__file__).parents[1] / "script" / "install.py"
    spec = importlib.util.spec_from_file_location("tdgpt_windows_install", module_path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.object(platform, "system", return_value="Windows"), mock.patch.dict(
        sys.modules, {"winreg": mock.MagicMock()}
    ):
        spec.loader.exec_module(module)
    return module


def make_installer(install_module, tmp_path):
    installer = install_module.WindowsInstaller.__new__(
        install_module.WindowsInstaller
    )
    installer.install_dir = tmp_path
    installer._archive_member_cache = {}
    installer.print_error = mock.Mock()
    installer.print_info = mock.Mock()
    installer.print_warning = mock.Mock()
    return installer


@pytest.mark.parametrize(
    "member_name", ["/absolute.txt", "C:/absolute.txt", "..\\escape.txt"]
)
def test_archive_member_validation_rejects_unsafe_paths(install_module, member_name):
    with pytest.raises(ValueError, match="Unsafe"):
        install_module.validate_archive_member_path(member_name)


def test_safe_extract_tar_rejects_parent_traversal_without_escaping(
    install_module, tmp_path
):
    archive_path = tmp_path / "malicious.tar"
    with tarfile.open(archive_path, "w") as archive:
        safe_data = b"safe"
        safe_member = tarfile.TarInfo("model/safe.txt")
        safe_member.size = len(safe_data)
        archive.addfile(safe_member, io.BytesIO(safe_data))

        escaped_data = b"escaped"
        escaped_member = tarfile.TarInfo("../outside-tar.txt")
        escaped_member.size = len(escaped_data)
        archive.addfile(escaped_member, io.BytesIO(escaped_data))

    destination = tmp_path / "tar-output"
    installer = make_installer(install_module, tmp_path)
    with mock.patch.object(install_module.shutil, "which", return_value=None):
        assert installer.extract_archive_to_dir(archive_path, destination) is False

    assert not (tmp_path / "outside-tar.txt").exists()
    assert (destination / "model" / "safe.txt").read_text() == "safe"
    assert "outside" in installer.print_error.call_args.args[0]


def test_safe_extract_zip_rejects_parent_traversal_before_writing(
    install_module, tmp_path
):
    archive_path = tmp_path / "malicious.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("model/safe.txt", "safe")
        archive.writestr("../outside-zip.txt", "escaped")

    destination = tmp_path / "zip-output"
    installer = make_installer(install_module, tmp_path)
    assert installer.extract_archive_to_dir(archive_path, destination) is False

    assert not (tmp_path / "outside-zip.txt").exists()
    assert not (destination / "model" / "safe.txt").exists()
    assert "parent traversal" in installer.print_error.call_args.args[0]


def test_safe_extract_allows_normal_tar_and_zip_members(install_module, tmp_path):
    tar_path = tmp_path / "normal.tar"
    with tarfile.open(tar_path, "w") as archive:
        data = b"tar"
        member = tarfile.TarInfo("model/tar.txt")
        member.size = len(data)
        archive.addfile(member, io.BytesIO(data))

    tar_destination = tmp_path / "normal-tar"
    installer = make_installer(install_module, tmp_path)
    with mock.patch.object(install_module.shutil, "which", return_value=None):
        assert installer.extract_archive_to_dir(tar_path, tar_destination) is True

    zip_path = tmp_path / "normal.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("model/zip.txt", "zip")

    zip_destination = tmp_path / "normal-zip"
    assert installer.extract_archive_to_dir(zip_path, zip_destination) is True

    assert (tar_destination / "model" / "tar.txt").read_text() == "tar"
    assert (zip_destination / "model" / "zip.txt").read_text() == "zip"


def test_tar_fast_path_does_not_prescan_archive(install_module, tmp_path):
    archive_path = tmp_path / "model.tar.gz"
    archive_path.touch()
    destination = tmp_path / "output"
    installer = make_installer(install_module, tmp_path)
    installer.get_archive_member_names = mock.Mock(
        side_effect=AssertionError("archive must not be pre-scanned")
    )

    completed = mock.Mock(returncode=0, stdout="", stderr="")
    with mock.patch.object(
        install_module.shutil, "which", return_value="C:/Windows/System32/tar.exe"
    ), mock.patch.object(
        install_module.subprocess, "run", return_value=completed
    ) as run:
        assert installer.extract_archive_to_dir(archive_path, destination) is True

    installer.get_archive_member_names.assert_not_called()
    run.assert_called_once()
    assert run.call_args.args[0][1] == "-xf"
