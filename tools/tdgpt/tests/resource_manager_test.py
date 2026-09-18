import hashlib
import io
import json
import tarfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from urllib.error import URLError

import pytest
from resource_manager import DynamicResourceManager, ResourceImportError, lifecycle_lock
from resource_manifest import SHARED_RUNTIME_NAME, validate_tsfm_model


def _runtime_archive(tmp_path):
    source = Path(tmp_path) / "source"
    root = source / "model_venvs" / "tsfm"
    python_exe = root / "bin" / "python3"
    python_exe.parent.mkdir(parents=True, exist_ok=True)
    python_exe.write_text("#!/bin/sh\necho 3.12.12\n", encoding="utf-8")
    python_exe.chmod(0o755)
    lock = b"torch==2.4.1\n"
    (root / "requirements.lock").write_bytes(lock)
    (root / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "kind": "tsfm-runtime", "runtime_id": "tsfm-runtime-1.0.0",
        "python_version": "3.12.12", "platform": "linux", "arch": "x64",
        "requirements_sha256": hashlib.sha256(lock).hexdigest(),
    }), encoding="utf-8")
    archive = Path(tmp_path) / "runtime.tar.gz"
    with tarfile.open(archive, "w:gz") as output:
        output.add(root, arcname="model_venvs/tsfm")
    return archive


def _model_archive(tmp_path, name="moirai"):
    archive = Path(tmp_path) / f"{name}.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr(f"model/{name}/config.json", "{}")
        output.writestr(f"model/{name}/model.safetensors", "weights")
    return archive


def _release_manifest(tmp_path, runtime, models):
    manifest = {
        "schema_version": 1,
        "release": "1.0.0",
        "runtimes": [{
            "kind": "runtime",
            "name": "tsfm_runtime",
            "version": "1.0.0",
            "platform": "linux",
            "arch": "x64",
            "file": runtime.name,
            "size": runtime.stat().st_size,
            "sha256": hashlib.sha256(runtime.read_bytes()).hexdigest(),
        }],
        "models": [{
            "kind": "model",
            "name": name,
            "model_id": f"example/{name}-small",
            "model_version": f"{name}-small",
            "version": "1.0.0",
            "file": archive.name,
            "size": archive.stat().st_size,
            "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
            "required_runtime": "1.0.0",
            "note": f"{name} test model",
        } for name, archive in models.items()],
    }
    path = Path(tmp_path) / "release.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_import_runtime_and_model_atomically(tmp_path, mock_config, mocker):
    mock_config.data_dir = str(Path(tmp_path) / "data")
    mock_config.tsfm_venv = str(Path(mock_config.data_dir) / "model_venvs" / "tsfm")
    mock_config.tsfm_model_dir = str(Path(mock_config.data_dir) / "model" / "tsfm")
    runtime = Path(tmp_path) / "python-runtime" / "bin"
    runtime.mkdir(parents=True)
    runtime_python = runtime / "python3"
    runtime_python.write_text("#!/bin/sh\necho 3.12.12\n", encoding="utf-8")
    runtime_python.chmod(0o755)
    mock_config.python_runtime_dir = str(runtime.parent)
    mocker.patch("resource_manifest.normalized_platform", return_value="linux")
    mocker.patch("resource_manifest.normalized_arch", return_value="x64")
    manager = DynamicResourceManager(mock_config)
    result = manager.import_packages([str(_runtime_archive(tmp_path)), str(_model_archive(tmp_path))])
    assert [entry["status"] for entry in result] == ["imported", "imported"]
    assert (Path(mock_config.tsfm_venv) / ".tdgpt-install-complete.json").is_file()
    assert (Path(mock_config.tsfm_model_dir) / "moirai" / "config.json").is_file()
    assert manager.import_packages([str(_runtime_archive(tmp_path))])[0]["status"] == "unchanged"


def test_import_rejects_bad_archive_hash(tmp_path, mock_config):
    with pytest.raises(ResourceImportError, match="SHA-256"):
        DynamicResourceManager(mock_config).import_packages([str(_model_archive(tmp_path))], "0" * 64)


def test_extract_reports_the_archive_total_when_one_member_is_skipped(
    tmp_path, mock_config, mocker
):
    class Entry:
        def __init__(self, name, size):
            self.name = name
            self.size = size
            self.mode = 0o644

        def isfile(self):
            return True

    entries = [
        Entry("model/moirai/config.json", 2),
        Entry("model/moirai/model.safetensors", 7),
    ]

    class Archive:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def getmembers(self):
            return entries

        def extractfile(self, entry):
            return io.BytesIO(b"{}") if entry is entries[0] else None

    package = Path(tmp_path) / "model.tar"
    package.touch()
    mocker.patch("resource_manager.zipfile.is_zipfile", return_value=False)
    mocker.patch("resource_manager.tarfile.open", return_value=Archive())
    progress = []

    DynamicResourceManager(mock_config)._extract(
        package,
        "model/moirai/",
        Path(tmp_path) / "staging",
        lambda done, total: progress.append((done, total)),
    )

    assert progress[-1] == (2, 9)


def test_model_validation_requires_model_markers(tmp_path):
    model_dir = Path(tmp_path) / "moment"
    model_dir.mkdir()
    (model_dir / "README.md").write_text("placeholder", encoding="utf-8")
    with pytest.raises(ValueError, match="missing model.safetensors"):
        validate_tsfm_model(model_dir, "moment", {})


def test_runtime_name_is_canonicalised_for_legacy_releases(tmp_path, mock_config, mocker):
    """A release published before the rename still names the runtime "tsfm"."""
    mocker.patch("resource_manager.normalized_platform", return_value="linux")
    mocker.patch("resource_manager.normalized_arch", return_value="x64")
    mocker.patch("resource_manifest.normalized_platform", return_value="linux")
    mocker.patch("resource_manifest.normalized_arch", return_value="x64")
    mock_config.data_dir = str(Path(tmp_path) / "data")
    mock_config.tsfm_venv = str(Path(mock_config.data_dir) / "model_venvs" / "tsfm")
    mock_config.tsfm_model_dir = str(Path(mock_config.data_dir) / "model" / "tsfm")
    runtime_python_dir = tmp_path / "python-runtime" / "bin"
    runtime_python_dir.mkdir(parents=True)
    runtime_python = runtime_python_dir / "python3"
    runtime_python.write_text("#!/bin/sh\necho 3.12.12\n", encoding="utf-8")
    runtime_python.chmod(0o755)
    mock_config.python_runtime_dir = str(runtime_python_dir.parent)

    runtime = _runtime_archive(tmp_path)
    release = _release_manifest(tmp_path, runtime, {})
    value = json.loads(release.read_text(encoding="utf-8"))
    value["runtimes"][0]["name"] = "tsfm"
    release.write_text(json.dumps(value), encoding="utf-8")

    manager = DynamicResourceManager(mock_config)

    # Reported name and apply selector both use the canonical name.
    resources = manager.check_release(str(release), "", SHARED_RUNTIME_NAME)["resources"]
    assert [item["name"] for item in resources] == [SHARED_RUNTIME_NAME]
    assert manager.apply_release(str(release), "", SHARED_RUNTIME_NAME) == [
        {
            "status": "imported",
            "kind": "runtime",
            "name": SHARED_RUNTIME_NAME,
            "path": str(Path(mock_config.tsfm_venv)),
        }
    ]

    # The recorded task file uses it too: that is what the download progress
    # poller matches on.
    task_root = Path(mock_config.data_dir) / "resource" / "state" / "tasks"
    tasks = sorted(task_root.glob("*.json"))
    assert [json.loads(path.read_text(encoding="utf-8"))["name"] for path in tasks] == [
        SHARED_RUNTIME_NAME
    ]


def test_delete_model_and_runtime(tmp_path, mock_config):
    mock_config.data_dir = str(Path(tmp_path) / "data")
    mock_config.model_venvs_dir = str(Path(tmp_path) / "model_venvs")
    mock_config.tsfm_venv = str(Path(mock_config.model_venvs_dir) / "tsfm")
    mock_config.tsfm_model_dir = str(Path(mock_config.data_dir) / "model" / "tsfm")
    model_dir = Path(mock_config.tsfm_model_dir) / "moirai"
    runtime_dir = Path(mock_config.tsfm_venv)
    model_dir.mkdir(parents=True)
    runtime_dir.mkdir(parents=True)
    (model_dir / "model.safetensors").write_bytes(b"weights")
    (model_dir / "config.json").write_text("{}", encoding="utf-8")
    (runtime_dir / "manifest.json").write_text("{}", encoding="utf-8")

    manager = DynamicResourceManager(mock_config)
    assert manager.delete_resource("moirai")["status"] == "deleted"
    assert not model_dir.exists()
    assert manager.delete_resource("tsfm_runtime")["status"] == "deleted"
    assert not runtime_dir.exists()


@pytest.mark.parametrize("target", ["", ".", "..", "../moment", "/tmp", "C:\\temp", "moment/..", "moment\\.."])
def test_delete_rejects_unsafe_targets(tmp_path, mock_config, target):
    manager = DynamicResourceManager(mock_config)
    manager.model_root.mkdir(parents=True)
    sentinel = manager.model_root / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    with pytest.raises(ResourceImportError, match="invalid resource name"):
        manager.delete_resource(target)
    assert sentinel.read_text() == "keep"


def test_delete_accepts_dotted_model_name(mock_config):
    manager = DynamicResourceManager(mock_config)
    result = manager.delete_resource("model.v1")
    assert result["status"] == "not_found"


def test_delete_rejects_symlink(tmp_path, mock_config):
    manager = DynamicResourceManager(mock_config)
    manager.model_root.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    (manager.model_root / "moment").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ResourceImportError, match="unsafe resource path"):
        manager.delete_resource("moment")
    assert (outside / "keep.txt").is_file()


def test_import_replaces_empty_runtime_root_created_by_lite_installer(tmp_path, mock_config, mocker):
    mock_config.data_dir = str(Path(tmp_path) / "data")
    mock_config.tsfm_venv = str(Path(mock_config.data_dir) / "model_venvs" / "tsfm")
    runtime_python = Path(tmp_path) / "python-runtime" / "bin" / "python3"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_text("#!/bin/sh\necho 3.12.12\n", encoding="utf-8")
    runtime_python.chmod(0o755)
    mock_config.python_runtime_dir = str(runtime_python.parent.parent)
    Path(mock_config.tsfm_venv).mkdir(parents=True)
    mocker.patch("resource_manager.normalized_platform", return_value="linux")
    mocker.patch("resource_manager.normalized_arch", return_value="x64")
    mocker.patch("resource_manifest.normalized_platform", return_value="linux")
    mocker.patch("resource_manifest.normalized_arch", return_value="x64")

    runtime = _runtime_archive(tmp_path)
    release = _release_manifest(tmp_path, runtime, {})
    result = DynamicResourceManager(mock_config).apply_release(str(release), "", "all")

    assert result[0]["status"] == "imported"
    assert (Path(mock_config.tsfm_venv) / "manifest.json").is_file()


def test_apply_release_reuses_runtime_and_cached_install(tmp_path, mock_config, mocker):
    mock_config.data_dir = str(Path(tmp_path) / "data")
    mock_config.tsfm_venv = str(Path(mock_config.data_dir) / "model_venvs" / "tsfm")
    mock_config.tsfm_model_dir = str(Path(mock_config.data_dir) / "model" / "tsfm")
    runtime_python = Path(tmp_path) / "python-runtime" / "bin" / "python3"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_text("#!/bin/sh\necho 3.12.12\n", encoding="utf-8")
    runtime_python.chmod(0o755)
    mock_config.python_runtime_dir = str(runtime_python.parent.parent)
    mocker.patch("resource_manager.normalized_platform", return_value="linux")
    mocker.patch("resource_manager.normalized_arch", return_value="x64")
    mocker.patch("resource_manifest.normalized_platform", return_value="linux")
    mocker.patch("resource_manifest.normalized_arch", return_value="x64")

    runtime = _runtime_archive(tmp_path)
    models = {
        "moirai": _model_archive(tmp_path, "moirai"),
        "moment": _model_archive(tmp_path, "moment"),
    }
    release = _release_manifest(tmp_path, runtime, models)
    release_value = json.loads(release.read_text(encoding="utf-8"))
    runtime_entry = release_value["runtimes"][0]
    model_entries = {item["name"]: item for item in release_value["models"]}
    manager = DynamicResourceManager(mock_config)

    # The shared runtime is a resource of its own: a model-scoped apply never
    # installs it implicitly, so it is installed first, as an operator would.
    assert [entry["status"] for entry in manager.apply_release(str(release), "", SHARED_RUNTIME_NAME)] == ["imported"]
    completion = Path(mock_config.tsfm_venv) / ".tdgpt-install-complete.json"
    completion_before = completion.read_bytes()
    assert manager.apply_release(str(release), "", SHARED_RUNTIME_NAME) == [
        {"status": "unchanged", "kind": "runtime", "name": SHARED_RUNTIME_NAME, "path": str(Path(mock_config.tsfm_venv))}
    ]

    first = manager.apply_release(str(release), "", "moirai")
    assert [entry["status"] for entry in first] == ["imported"]
    assert not list((Path(mock_config.data_dir) / "resource" / "cache").glob("*.archive"))
    deployed_at = manager.list_resources("moirai")["models"][0]["deployed_at"]

    second = manager.apply_release(str(release), "", "moment")
    assert [entry["status"] for entry in second] == ["imported"]
    assert not list((Path(mock_config.data_dir) / "resource" / "cache").glob("*.archive"))
    assert completion.read_bytes() == completion_before

    runtime.unlink()
    models["moirai"].unlink()
    repeated = manager.apply_release(str(release), "", "moirai")
    assert [entry["status"] for entry in repeated] == ["unchanged"]
    assert manager.list_resources("moirai")["models"][0]["deployed_at"] == deployed_at
    assert manager.check_release(str(release), "", "all")["resources"] == [
        {
            "kind": "runtime", "name": "tsfm_runtime", "model_id": None,
            "model_version": None, "version": "1.0.0",
            "digest": f"sha256:{runtime_entry['sha256']}",
            "size": runtime_entry["size"],
            "state": "installed", "note": "",
        },
        {
            "kind": "model", "name": "moirai", "model_id": "example/moirai-small",
            "model_version": "moirai-small", "version": "1.0.0",
            "digest": f"sha256:{model_entries['moirai']['sha256']}",
            "size": model_entries["moirai"]["size"],
            "state": "installed", "note": "moirai test model",
        },
        {
            "kind": "model", "name": "moment", "model_id": "example/moment-small",
            "model_version": "moment-small", "version": "1.0.0",
            "digest": f"sha256:{model_entries['moment']['sha256']}",
            "size": model_entries["moment"]["size"],
            "state": "installed", "note": "moment test model",
        },
    ]

    listed = manager.list_resources()
    assert listed["models"][0] == {
        "name": "moirai",
        "model_id": "example/moirai-small",
        "model_version": "moirai-small",
        "version": "1.0.0",
        "digest": f"sha256:{model_entries['moirai']['sha256']}",
        "size": model_entries["moirai"]["size"],
        "deployed_at": listed["models"][0]["deployed_at"],
        "install_status": "installed",
        "note": "moirai test model",
        "path": str(Path(mock_config.tsfm_model_dir) / "moirai"),
    }
    assert listed["models"][0]["deployed_at"]


def test_apply_release_rejects_wrong_archive_size(tmp_path, mock_config, mocker):
    mock_config.data_dir = str(Path(tmp_path) / "data")
    mock_config.tsfm_venv = str(Path(mock_config.data_dir) / "model_venvs" / "tsfm")
    mock_config.tsfm_model_dir = str(Path(mock_config.data_dir) / "model" / "tsfm")
    runtime_python = Path(tmp_path) / "python-runtime" / "bin" / "python3"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_text("#!/bin/sh\necho 3.12.12\n", encoding="utf-8")
    runtime_python.chmod(0o755)
    mock_config.python_runtime_dir = str(runtime_python.parent.parent)
    mocker.patch("resource_manager.normalized_platform", return_value="linux")
    mocker.patch("resource_manager.normalized_arch", return_value="x64")

    runtime = _runtime_archive(tmp_path)
    release = _release_manifest(tmp_path, runtime, {})
    manifest = json.loads(release.read_text(encoding="utf-8"))
    manifest["runtimes"][0]["size"] += 1
    release.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ResourceImportError, match="download size mismatch"):
        DynamicResourceManager(mock_config).apply_release(str(release), "", "all")


def test_resource_operation_rejects_concurrent_lock(mock_config):
    manager = DynamicResourceManager(mock_config)
    other = DynamicResourceManager(mock_config)
    manager._acquire_lock()
    try:
        with pytest.raises(ResourceImportError, match="another TDgpt resource operation"):
            other.import_packages([])
    finally:
        manager._release_lock()
    assert other.import_packages([]) == []


def test_lifecycle_lock_releases_after_failure(mock_config):
    with pytest.raises(ValueError):
        with lifecycle_lock(mock_config.data_dir):
            with pytest.raises(ResourceImportError, match="another TDgpt resource operation"):
                with lifecycle_lock(mock_config.data_dir):
                    pytest.fail("concurrent operation acquired lock")
            raise ValueError("operation failed")
    with lifecycle_lock(mock_config.data_dir):
        pass


def test_resource_lock_clears_state_when_enter_fails(mock_config, mocker):
    manager = DynamicResourceManager(mock_config)

    @contextmanager
    def failing_lock(*args, **kwargs):
        raise RuntimeError("lock setup failed")
        yield

    mocker.patch("resource_manager.lifecycle_lock", failing_lock)

    with pytest.raises(RuntimeError, match="lock setup failed"):
        manager._acquire_lock()

    assert manager._resource_lock is None


def test_resource_lock_remains_held_until_outer_release(mock_config):
    manager = DynamicResourceManager(mock_config)
    other = DynamicResourceManager(mock_config)
    manager._acquire_lock()
    manager._acquire_lock()
    manager._release_lock()
    try:
        with pytest.raises(ResourceImportError, match="another TDgpt resource operation"):
            other._acquire_lock()
    finally:
        manager._release_lock()
    other._acquire_lock()
    other._release_lock()


def test_delete_runtime_through_symlinked_parent(tmp_path, mock_config):
    actual_root = tmp_path / "actual"
    actual_runtime = actual_root / "model_venvs" / "tsfm"
    actual_runtime.mkdir(parents=True)
    (actual_runtime / "manifest.json").write_text("{}")
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(actual_root, target_is_directory=True)
    mock_config.model_venvs_dir = str(linked_root / "model_venvs")
    mock_config.tsfm_venv = str(linked_root / "model_venvs" / "tsfm")

    result = DynamicResourceManager(mock_config).delete_resource("runtime")

    assert result["status"] == "deleted"
    assert not actual_runtime.exists()


def test_delete_model_rejects_model_root_alias(mock_config):
    model_root = Path(mock_config.tsfm_model_dir)
    model_root.mkdir(parents=True)
    keep = model_root / "keep.txt"
    keep.write_text("keep", encoding="utf-8")

    with pytest.raises(ResourceImportError, match="invalid|unsafe"):
        DynamicResourceManager(mock_config).delete_resource(".")

    assert keep.read_text(encoding="utf-8") == "keep"


def test_delete_runtime_rejects_path_outside_model_venvs(tmp_path, mock_config):
    unsafe_runtime = tmp_path / "unsafe-runtime"
    unsafe_runtime.mkdir()
    (unsafe_runtime / "keep.txt").write_text("keep")
    mock_config.tsfm_venv = str(unsafe_runtime)

    with pytest.raises(ResourceImportError, match="unsafe resource path"):
        DynamicResourceManager(mock_config).delete_resource("runtime")

    assert (unsafe_runtime / "keep.txt").is_file()


def test_repeated_delete_clears_stale_state(mock_config):
    manager = DynamicResourceManager(mock_config)
    manager.state_root.mkdir(parents=True)
    manager.task_root.mkdir(parents=True)
    state = manager.state_root / "moment.json"
    state.write_text("{}")
    task = manager.task_root / "digest.json"
    task.write_text(json.dumps({"kind": "model", "name": "moment", "status": "completed"}))
    other = manager.task_root / "other.json"
    other.write_text(json.dumps({"kind": "model", "name": "moirai", "status": "completed"}))
    assert manager.delete_resource("moment")["status"] == "not_found"
    assert not state.exists()
    assert not task.exists()
    assert other.exists()
    assert manager.delete_resource("moment")["status"] == "not_found"


def test_delete_reports_state_cleanup_failure(mock_config, mocker):
    manager = DynamicResourceManager(mock_config)
    mocker.patch.object(
        manager,
        "_clear_deleted_state",
        side_effect=OSError("state is read-only"),
    )

    with pytest.raises(ResourceImportError, match="failed to clear state"):
        manager.delete_resource("moment")


def test_runtime_autodetects_current_python_base_prefix(tmp_path, mock_config, mocker):
    runtime_dir = Path(tmp_path) / "python-runtime"
    runtime_python = runtime_dir / "bin" / "python3"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_text("#!/bin/sh\necho 3.12.12\n", encoding="utf-8")
    mock_config.python_runtime_dir = None
    mock_config.venv_dir = str(Path(tmp_path) / "missing-main-venv")
    mocker.patch("resource_manager.sys.base_prefix", str(runtime_dir))
    mocker.patch("resource_manager.platform.system", return_value="Linux")

    manager = DynamicResourceManager(mock_config)

    assert manager.python_runtime_dir == runtime_dir.resolve()


def test_runtime_normalizes_pyvenv_home_bin_directory(tmp_path, mock_config, mocker):
    runtime_dir = Path(tmp_path) / "python-runtime"
    runtime_python = runtime_dir / "bin" / "python3"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_text("#!/bin/sh\necho 3.12.12\n", encoding="utf-8")
    main_venv = Path(tmp_path) / "main-venv"
    main_venv.mkdir()
    (main_venv / "pyvenv.cfg").write_text(
        f"home = {runtime_dir / 'bin'}\n",
        encoding="utf-8",
    )
    mock_config.python_runtime_dir = None
    mock_config.venv_dir = str(main_venv)
    mocker.patch("resource_manager.sys.base_prefix", str(Path(tmp_path) / "missing-runtime"))
    mocker.patch("resource_manager.platform.system", return_value="Linux")

    manager = DynamicResourceManager(mock_config)

    assert manager.python_runtime_dir == runtime_dir.resolve()


def test_release_manifest_falls_back_to_public_url(tmp_path, mock_config, mocker):
    manager = DynamicResourceManager(mock_config)
    fallback_manifest = json.dumps(
        {"schema_version": 1, "runtimes": [], "models": []}
    ).encode()

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return fallback_manifest

    requested = []

    def urlopen(request, timeout):
        requested.append(str(request))
        if "internal.example" in str(request):
            raise URLError("unavailable")
        return Response()

    mocker.patch("resource_manager.urllib.request.urlopen", side_effect=urlopen)

    manifest = manager.load_release("1.0.0", "https://internal.example")

    assert manifest["_source_url"] == (
        "https://downloads.taosdata.com/tdengine-tdgpt-enterprise/tsfm/1.0.0/"
        "tdengine-tdgpt-tsfm-release-1.0.0.json"
    )
    assert requested == [
        "https://internal.example/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-tsfm-release-1.0.0.json",
        manifest["_source_url"],
    ]


def test_resource_download_falls_back_to_matching_release_directory(
    tmp_path, mock_config, mocker
):
    payload = b"runtime archive"
    digest = hashlib.sha256(payload).hexdigest()
    manager = DynamicResourceManager(mock_config)
    manager._fallback_release_source = (
        "https://downloads.taosdata.com/tdengine-tdgpt-enterprise/tsfm/1.0.0/"
        "tdengine-tdgpt-tsfm-release-1.0.0.json"
    )
    requested = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def getcode(self):
            return 200

        def read(self, _size):
            if hasattr(self, "sent"):
                return b""
            self.sent = True
            return payload

    def urlopen(request, timeout):
        url = request.full_url
        requested.append(url)
        if "internal.example" in url:
            raise URLError("unavailable")
        return Response()

    mocker.patch("resource_manager.urllib.request.urlopen", side_effect=urlopen)

    archive = manager._download(
        {
            "name": "tsfm_runtime",
            "file": "tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz",
            "size": len(payload),
            "sha256": digest,
        },
        "https://internal.example/tdengine-tdgpt-enterprise/tsfm/1.0.0/"
        "tdengine-tdgpt-tsfm-release-1.0.0.json",
    )

    assert archive.read_bytes() == payload
    assert requested == [
        "https://internal.example/tdengine-tdgpt-enterprise/tsfm/1.0.0/"
        "tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz",
        "https://downloads.taosdata.com/tdengine-tdgpt-enterprise/tsfm/1.0.0/"
        "tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz",
    ]
