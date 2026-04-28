import release


def test_get_cpu_type_normalizes_aarch64_to_arm64(monkeypatch):
    monkeypatch.setattr(release.platform, "architecture", lambda: ("64bit", ""))
    monkeypatch.setattr(release.platform, "machine", lambda: "aarch64")

    assert release.GetCpuType() == "arm64"


def test_normalize_cpu_type_maps_aarch64_flag_to_arm64():
    assert release.normalize_cpu_type("aarch64") == "arm64"
    assert release.normalize_cpu_type("arm64") == "arm64"
    assert release.normalize_cpu_type("x64") == "x64"


def test_init_build_info_preserves_explicit_only_build_path(monkeypatch, tmp_path):
    monkeypatch.setattr(release.platform, "architecture", lambda: ("64bit", ""))
    monkeypatch.setattr(release.platform, "machine", lambda: "aarch64")
    monkeypatch.setattr(
        release.sys,
        "argv",
        [
            "release.py",
            "--only_build",
            str(tmp_path / "package_inputs" / "taosx"),
            "-vn",
            "3.4.1.4.0417",
        ],
    )
    monkeypatch.setattr(release, "sub_module", [])
    monkeypatch.setattr(release, "test_process", "")
    monkeypatch.setattr(release, "release_info", release.ReleaseInfo("Linux"))

    release.init_build_info()

    assert release.release_info.InstallPath == str(tmp_path / "package_inputs" / "taosx")
