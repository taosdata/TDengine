"""Atomic local importer for TDgpt dynamic TSFM resources."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from resource_manifest import (
    COMPLETION_FILE,
    SHA256_PATTERN,
    load_manifest,
    normalized_arch,
    normalized_platform,
    validate_tsfm_model,
    validate_tsfm_runtime,
)


class ResourceImportError(RuntimeError):
    """Raised when a dynamic resource package cannot be imported safely."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relative(name: str) -> Path:
    value = Path(name.replace("\\", "/"))
    if value.is_absolute() or ".." in value.parts:
        raise ResourceImportError(f"unsafe archive entry: {name}")
    return value


class DynamicResourceManager:
    """Import official TSFM runtime and model archives without copying live trees."""

    def __init__(self, config: Any):
        self.model_root = Path(config.tsfm_model_dir)
        self.runtime_root = Path(config.tsfm_venv)
        self.resource_root = Path(config.data_dir) / "resource"
        self.state_root = self.resource_root / "state" / "models"
        self.task_root = self.resource_root / "state" / "tasks"
        self.lock_path = self.resource_root / ".tdgpt-resource.lock"
        self.python_runtime_dir = self._resolve_python_runtime_dir(config)
        self.cache_root = self.resource_root / "cache"
        self._fallback_base_url = "https://downloads.taosdata.com"
        self._active_base_url = ""
        self._fallback_release_source = ""

    def _release_url(self, release: str, base_url: str) -> str:
        if Path(release).is_file():
            return str(Path(release).resolve())
        if release.startswith("http://") or release.startswith("https://"):
            return release
        base = base_url.rstrip("/")
        return f"{base}/tdengine-tdgpt-enterprise/tsfm/{release}/tdengine-tdgpt-tsfm-release-{release}.json"

    def load_release(self, release: str, base_url: str) -> Dict[str, Any]:
        source = self._release_url(release, base_url)
        sources = [source]
        if (
            source.startswith(("http://", "https://"))
            and not release.startswith(("http://", "https://"))
            and base_url.rstrip("/") != self._fallback_base_url
        ):
            self._fallback_release_source = self._release_url(
                release, self._fallback_base_url
            )
            sources.append(self._fallback_release_source)
        else:
            self._fallback_release_source = ""
        last_error = None
        selected_fallback = False
        for candidate in sources:
            try:
                if candidate.startswith(("http://", "https://")):
                    with urllib.request.urlopen(candidate, timeout=30) as response:
                        value = json.loads(response.read().decode("utf-8"))
                else:
                    value = json.loads(Path(candidate).read_text(encoding="utf-8"))
                source = candidate
                if candidate != sources[0]:
                    selected_fallback = True
                    print(f"Primary resource URL failed; using fallback URL: {candidate}")
                break
            except (OSError, urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
                last_error = exc
        else:
            raise ResourceImportError(f"resource release manifest unavailable: {sources[0]}: {last_error}") from last_error
        if not isinstance(value, dict) or value.get("schema_version") != 1:
            raise ResourceImportError("unsupported resource release manifest")
        value["_source_url"] = source
        self._active_base_url = self._fallback_base_url if selected_fallback else base_url.rstrip("/")
        return value

    @staticmethod
    def _resource_entries(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
        entries = list(manifest.get("runtimes", []))
        entries.extend(manifest.get("models", []))
        if not all(isinstance(item, dict) for item in entries):
            raise ResourceImportError("release manifest contains invalid resources")
        return entries

    def _select_resources(self, manifest: Dict[str, Any], target: str) -> List[Dict[str, Any]]:
        current_platform = normalized_platform()
        current_arch = normalized_arch()
        selected = []
        for item in self._resource_entries(manifest):
            kind = item.get("kind", "model")
            if kind == "runtime" and (item.get("platform") != current_platform or item.get("arch") != current_arch):
                continue
            if target != "all" and kind == "model" and item.get("name") != target:
                continue
            selected.append(item)
        if target != "all" and not any(item.get("name") == target for item in selected):
            raise ResourceImportError(f"resource not found in release manifest: {target}")
        return selected

    def _download(self, item: Dict[str, Any], source_url: str) -> Path:
        digest = str(item.get("sha256", "")).lower()
        if not SHA256_PATTERN.fullmatch(digest):
            raise ResourceImportError(f"invalid SHA-256 in release manifest for {item.get('name')}")
        expected_size = self._expected_size(item)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        archive = self.cache_root / f"{digest}.archive"
        task = self._task_path(digest)
        self._update_task(task, item, "downloading", 0, expected_size)
        if archive.is_file() and self._size_matches(archive, expected_size) and sha256_file(archive) == digest:
            self._update_task(task, item, "downloaded", expected_size, expected_size)
            return archive
        relative_file = _safe_relative(str(item["file"]))
        if relative_file.as_posix() != str(item["file"]).replace("\\", "/"):
            raise ResourceImportError(f"invalid resource file path: {item['file']}")
        if source_url.startswith(("http://", "https://")):
            url = urllib.parse.urljoin(source_url.rsplit("/", 1)[0] + "/", relative_file.as_posix())
        else:
            url = str(Path(source_url).parent / relative_file)
        part = self.cache_root / f"{digest}.part"
        if not url.startswith(("http://", "https://")):
            shutil.copyfile(url, part)
            self._update_task(task, item, "verifying", part.stat().st_size, expected_size)
            self._validate_download_size(part, expected_size, item)
            if sha256_file(part) != digest:
                raise ResourceImportError(f"download SHA-256 mismatch: {item.get('name')}")
            os.replace(part, archive)
            self._update_task(task, item, "downloaded", expected_size or archive.stat().st_size, expected_size)
            return archive
        if part.is_file() and expected_size and part.stat().st_size == expected_size:
            if sha256_file(part) == digest:
                os.replace(part, archive)
                return archive
            part.unlink()
        if part.is_file() and expected_size and part.stat().st_size > expected_size:
            part.unlink()
        request = urllib.request.Request(url)
        existing_size = part.stat().st_size if part.exists() else 0
        last_reported_size = existing_size
        last_reported_at = time.monotonic()
        if existing_size:
            request.add_header("Range", f"bytes={existing_size}-")
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                append = existing_size > 0 and response.getcode() == 206
                with part.open("ab" if append else "wb") as output:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)
                        current_size = part.stat().st_size
                        now = time.monotonic()
                        if current_size - last_reported_size >= 16 * 1024 * 1024 or now - last_reported_at >= 1:
                            self._update_task(task, item, "downloading", current_size, expected_size)
                            last_reported_size = current_size
                            last_reported_at = now
            self._update_task(task, item, "verifying", part.stat().st_size, expected_size)
            self._validate_download_size(part, expected_size, item)
            if sha256_file(part) != digest:
                raise ResourceImportError(f"download SHA-256 mismatch: {item.get('name')}")
            os.replace(part, archive)
            self._update_task(task, item, "downloaded", expected_size or archive.stat().st_size, expected_size)
            return archive
        except Exception as exc:
            if (
                url.startswith(("http://", "https://"))
                and self._fallback_release_source
                and isinstance(exc, (OSError, urllib.error.URLError, urllib.error.HTTPError))
            ):
                print(f"Primary resource download failed; using fallback URL for {item.get('name')}")
                fallback_source = self._fallback_release_source
                self._fallback_release_source = ""
                return self._download(item, fallback_source)
            self._update_task(task, item, "failed", part.stat().st_size if part.exists() else 0, expected_size, str(exc))
            if isinstance(exc, ResourceImportError):
                raise
            raise ResourceImportError(f"resource download failed: {url}: {exc}") from exc

    @staticmethod
    def _expected_size(item: Dict[str, Any]) -> int:
        value = item.get("size")
        if value is None:
            return 0
        if isinstance(value, bool):
            raise ResourceImportError(f"invalid resource size for {item.get('name')}")
        try:
            size = int(value)
        except (TypeError, ValueError) as exc:
            raise ResourceImportError(f"invalid resource size for {item.get('name')}") from exc
        if size <= 0:
            raise ResourceImportError(f"invalid resource size for {item.get('name')}")
        return size

    @staticmethod
    def _size_matches(path: Path, expected_size: int) -> bool:
        return not expected_size or path.stat().st_size == expected_size

    def _task_path(self, digest: str) -> Path:
        return self.task_root / f"{digest}.json"

    def _update_task(
        self,
        path: Path,
        item: Dict[str, Any],
        status: str,
        bytes_done: int = 0,
        bytes_total: int = 0,
        error: str = "",
    ) -> None:
        value = {
            "kind": item.get("kind", "model"),
            "name": item.get("name", "tsfm"),
            "model_id": item.get("model_id"),
            "model_version": item.get("model_version"),
            "version": item.get("version"),
            "digest": f"sha256:{item.get('sha256', '')}",
            "size": item.get("size"),
            "status": status,
            "bytes_done": bytes_done,
            "bytes_total": bytes_total,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if error:
            value["error"] = error
        try:
            self._write_json(path, value)
        except OSError:
            pass

    def _validate_download_size(self, path: Path, expected_size: int, item: Dict[str, Any]) -> None:
        if not self._size_matches(path, expected_size):
            raise ResourceImportError(
                f"download size mismatch for {item.get('name')}: "
                f"expected {expected_size}, got {path.stat().st_size}"
            )

    def check_release(self, release: str, base_url: str, target: str = "all") -> Dict[str, Any]:
        manifest = self.load_release(release, base_url)
        result = []
        for item in self._select_resources(manifest, target):
            kind = item.get("kind", "model")
            path = self.runtime_root if kind == "runtime" else self.model_root / item["name"]
            state = "installed" if self._installed_matches(item) else ("outdated" if path.exists() else "missing")
            task = {}
            try:
                task = load_manifest(self._task_path(str(item["sha256"])))
            except (OSError, ValueError, json.JSONDecodeError):
                pass
            task_status = task.get("status")
            if task_status in {"downloading", "verifying", "importing", "failed"}:
                state = task_status
            entry = {
                "kind": kind,
                "name": item.get("name", "tsfm"),
                "model_id": item.get("model_id"),
                "model_version": item.get("model_version"),
                "version": item.get("version"),
                "digest": f"sha256:{item.get('sha256', '')}",
                "size": item.get("size"),
                "state": state,
                "note": item.get("note", ""),
            }
            if task_status in {"downloading", "verifying", "importing", "failed"}:
                entry.update({
                    "task_status": task_status,
                    "bytes_done": task.get("bytes_done"),
                    "bytes_total": task.get("bytes_total"),
                })
                if task.get("error"):
                    entry["error"] = task["error"]
            result.append(entry)
        return {"release": manifest.get("release"), "resources": result}

    def _installed_matches(self, item: Dict[str, Any]) -> bool:
        digest = str(item.get("sha256", "")).lower()
        kind = item.get("kind", "model")
        try:
            if kind == "runtime":
                validate_tsfm_runtime(self.runtime_root)
                completion = load_manifest(self.runtime_root / COMPLETION_FILE)
                return completion.get("archive_sha256", "").lower() == digest
            name = str(item["name"])
            validate_tsfm_model(self.model_root / name, name, {})
            state = load_manifest(self.state_root / f"{name}.json")
            return state.get("archive_sha256", "").lower() == digest
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            return False

    def apply_release(self, release: str, base_url: str, target: str = "all", replace: bool = False) -> List[Dict[str, Any]]:
        manifest = self.load_release(release, base_url)
        source_url = manifest["_source_url"]
        items = self._select_resources(manifest, target)
        self._acquire_lock()
        try:
            downloads = {
                str(item["sha256"]): self._download(item, source_url)
                for item in items if not self._installed_matches(item)
            }
            results = []
            for item in items:
                kind = item.get("kind", "model")
                target_path = self.runtime_root if kind == "runtime" else self.model_root / str(item["name"])
                if self._installed_matches(item):
                    if kind == "model":
                        self._record_model_state(item, target_path, str(item["sha256"]))
                    results.append({"status": "unchanged", "kind": kind, "name": item.get("name", "tsfm"), "path": str(target_path)})
                    continue
                if target_path.exists() and not replace and (
                    not target_path.is_dir() or any(target_path.iterdir())
                ):
                    raise ResourceImportError(f"resource differs from release; rerun with --replace: {item.get('name')}")
                results.append(self.import_package(
                    str(downloads[str(item["sha256"])]), str(item["sha256"]), replace, item
                ))
            return results
        finally:
            self._release_lock()

    def requires_runtime_replacement(self, release: str, base_url: str, target: str = "all") -> bool:
        """Return whether applying this release would replace the shared runtime."""
        manifest = self.load_release(release, base_url)
        return any(
            item.get("kind", "model") == "runtime" and not self._installed_matches(item)
            for item in self._select_resources(manifest, target)
        )

    def verify_resources(self, target: str = "all") -> Dict[str, Any]:
        results = []
        try:
            manifest, _ = validate_tsfm_runtime(self.runtime_root)
            results.append({"kind": "runtime", "name": "tsfm", "status": "valid", "runtime_id": manifest["runtime_id"]})
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            results.append({"kind": "runtime", "name": "tsfm", "status": "invalid", "reason": str(exc)})
        names = [] if not self.model_root.is_dir() else sorted(path.name for path in self.model_root.iterdir() if path.is_dir())
        if target != "all":
            names = [target]
        for name in names:
            try:
                validate_tsfm_model(self.model_root / name, name, {})
                results.append({"kind": "model", "name": name, "status": "valid"})
            except (OSError, ValueError) as exc:
                results.append({"kind": "model", "name": name, "status": "invalid", "reason": str(exc)})
        return {"resources": results, "valid": all(item["status"] == "valid" for item in results)}

    def _resolve_python_runtime_dir(self, config: Any) -> Path | None:
        """Find the host Python runtime used to relocate imported venvs."""
        configured = getattr(config, "python_runtime_dir", None)
        candidates: List[Path] = []
        if configured:
            candidates.append(Path(configured).expanduser())

        # The main venv records its base runtime in pyvenv.cfg. Only use it
        # when the recorded directory still exists on this host.
        main_cfg = self.venv_cfg_path(config)
        if main_cfg.is_file():
            for line in main_cfg.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.lower().startswith("home ="):
                    candidates.append(Path(line.split("=", 1)[1].strip()))
                    break

        candidates.append(Path(sys.base_prefix))

        if platform.system().lower() == "windows":
            candidates.append(Path(r"C:\TDengine\deps\python-runtime"))
        else:
            candidates.append(Path("/usr/local/taos-deps/python-runtime"))
        for candidate in candidates:
            for runtime_dir in self._runtime_dir_candidates(candidate):
                if self.runtime_python(runtime_dir).is_file():
                    return runtime_dir.resolve()
        return None

    @staticmethod
    def _runtime_dir_candidates(candidate: Path) -> List[Path]:
        """Return possible runtime roots for a prefix or pyvenv.cfg home."""
        result = [candidate.expanduser()]
        directory_name = candidate.name.lower()
        if directory_name == "bin" or (
            platform.system().lower() == "windows" and directory_name == "scripts"
        ):
            result.append(candidate.parent)
        return result

    @staticmethod
    def venv_cfg_path(config: Any) -> Path:
        return Path(config.venv_dir) / "pyvenv.cfg"

    @staticmethod
    def runtime_python(runtime_dir: Path) -> Path:
        if platform.system().lower() == "windows":
            return runtime_dir / "python.exe"
        for name in ("python3", "python3.12", "python"):
            candidate = runtime_dir / "bin" / name
            if candidate.exists():
                return candidate
        return runtime_dir / "bin" / "python3"

    def _relocate_runtime(self, runtime_dir: Path) -> None:
        """Make an unpacked venv usable from this host's runtime location."""
        if self.python_runtime_dir is None:
            raise ResourceImportError("host Python runtime was not found")
        cfg_path = runtime_dir / "pyvenv.cfg"
        if cfg_path.is_file():
            values = {}
            for line in cfg_path.read_text(encoding="utf-8", errors="replace").splitlines():
                if "=" in line:
                    key, value = line.split("=", 1)
                    values[key.strip().lower()] = value.strip()
            values["home"] = str(self.python_runtime_dir)
            values["executable"] = str(self.runtime_python(self.python_runtime_dir))
            lines = [f"{key} = {value}" for key, value in values.items()]
            cfg_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        if platform.system().lower() != "windows":
            bin_dir = runtime_dir / "bin"
            bin_dir.mkdir(parents=True, exist_ok=True)
            target = self.runtime_python(self.python_runtime_dir)
            for name in ("python", "python3", "python3.12"):
                link = bin_dir / name
                if link.is_symlink() or link.exists():
                    link.unlink()
                link.symlink_to(target)

    def _acquire_lock(self) -> None:
        self.resource_root.mkdir(parents=True, exist_ok=True)

        # Check for stale lock — if lock file exists but process is dead, remove it
        if self.lock_path.exists():
            try:
                old_pid = int(self.lock_path.read_text().strip())
                os.kill(old_pid, 0)  # Signal 0: check if process exists
            except (OSError, ProcessLookupError, ValueError):
                # Process is dead or PID is invalid, remove stale lock
                self.lock_path.unlink(missing_ok=True)

        # Create lock file (exclusive create, fails if exists)
        try:
            with open(self.lock_path, "x", encoding="utf-8") as f:
                f.write(str(os.getpid()))
        except FileExistsError as exc:
            raise ResourceImportError("another TDgpt resource operation is running") from exc

    def _release_lock(self) -> None:
        self.lock_path.unlink(missing_ok=True)

    def _cleanup_cached_archive(self, package: Path) -> None:
        """Remove a completed download after its contents are installed."""
        try:
            if package.parent.resolve() == self.cache_root.resolve() and package.name.endswith(".archive"):
                package.unlink(missing_ok=True)
        except OSError:
            pass

    def _archive_entries(self, package: Path) -> List[str]:
        if zipfile.is_zipfile(package):
            with zipfile.ZipFile(package) as archive:
                return [entry.filename for entry in archive.infolist() if not entry.is_dir()]
        if tarfile.is_tarfile(package):
            with tarfile.open(package) as archive:
                return [entry.name for entry in archive.getmembers() if entry.isfile()]
        raise ResourceImportError(f"unsupported resource archive: {package}")

    def _classify(self, package: Path) -> Tuple[str, str]:
        entries = [_safe_relative(name).as_posix() for name in self._archive_entries(package)]
        if any(name.startswith("model_venvs/tsfm/") for name in entries):
            return "runtime", "tsfm"
        models = {name.split("/", 2)[1] for name in entries if name.startswith("model/") and name.count("/") >= 2}
        if len(models) != 1:
            raise ResourceImportError("model archive must contain exactly one model/<name>/ root")
        model_name = next(iter(models))
        if not model_name or "/" in model_name:
            raise ResourceImportError("invalid model archive root")
        return "model", model_name

    def _extract(self, package: Path, prefix: str, destination: Path) -> None:
        destination.mkdir(parents=True, exist_ok=False)
        prefix = prefix.rstrip("/") + "/"

        def write_file(relative: Path, reader: Any, mode: int) -> None:
            output = destination / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            with output.open("wb") as file_obj:
                shutil.copyfileobj(reader, file_obj, length=1024 * 1024)
            if mode:
                output.chmod(mode)

        extracted = 0
        if zipfile.is_zipfile(package):
            with zipfile.ZipFile(package) as archive:
                for entry in archive.infolist():
                    name = _safe_relative(entry.filename).as_posix()
                    if entry.is_dir() or not name.startswith(prefix):
                        continue
                    relative = _safe_relative(name[len(prefix) :])
                    if not relative.parts:
                        continue
                    with archive.open(entry) as source:
                        write_file(relative, source, (entry.external_attr >> 16) & 0o777)
                    extracted += 1
        else:
            with tarfile.open(package) as archive:
                for entry in archive.getmembers():
                    name = _safe_relative(entry.name).as_posix()
                    if not entry.isfile() or not name.startswith(prefix):
                        continue
                    relative = _safe_relative(name[len(prefix) :])
                    source = archive.extractfile(entry)
                    if source is None:
                        continue
                    with source:
                        write_file(relative, source, entry.mode & 0o777)
                    extracted += 1
        if extracted == 0:
            raise ResourceImportError(f"archive does not contain {prefix}")

    @staticmethod
    def _write_json(path: Path, value: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as file_obj:
            json.dump(value, file_obj, indent=2)
            file_obj.write("\n")
            temporary = Path(file_obj.name)
        os.replace(temporary, path)

    def _activate(self, staging: Path, target: Path) -> None:
        backup = target.parent / f".{target.name}.tdgpt-backup-{uuid.uuid4().hex}"
        had_target = target.exists()
        if had_target and target.is_dir() and not any(target.iterdir()):
            target.rmdir()
            had_target = False
        try:
            if had_target:
                os.replace(target, backup)
            os.replace(staging, target)
        except OSError:
            if had_target and backup.exists() and not target.exists():
                os.replace(backup, target)
            raise
        shutil.rmtree(backup, ignore_errors=True)

    def _runtime_complete(self, runtime_dir: Path, archive_hash: str) -> Dict[str, Any]:
        manifest, _ = validate_tsfm_runtime(runtime_dir, require_completion=False)
        completion = {
            "kind": "tsfm-runtime-install",
            "runtime_id": manifest["runtime_id"],
            "archive_sha256": archive_hash,
        }
        self._write_json(runtime_dir / COMPLETION_FILE, completion)
        validate_tsfm_runtime(runtime_dir)
        return manifest

    def _record_model_state(
        self,
        metadata: Dict[str, Any],
        target: Path,
        archive_hash: str,
        deployed_at: str | None = None,
    ) -> None:
        state_path = self.state_root / f"{metadata['name']}.json"
        if deployed_at is None and state_path.is_file():
            try:
                deployed_at = load_manifest(state_path).get("deployed_at")
            except (OSError, ValueError, json.JSONDecodeError):
                deployed_at = None
        self._write_json(state_path, {
            "kind": "tsfm-model-install",
            "model_name": metadata["name"],
            "model_id": metadata.get("model_id"),
            "model_version": metadata.get("model_version"),
            "resource_version": metadata.get("version"),
            "archive_file": metadata.get("file"),
            "archive_sha256": archive_hash,
            "size": metadata.get("size"),
            "path": str(target),
            "deployed_at": deployed_at or datetime.now(timezone.utc).isoformat(),
            "note": metadata.get("note", ""),
        })

    def import_package(
        self,
        package_path: str,
        expected_sha256: str = "",
        replace: bool = False,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        package = Path(package_path).resolve()
        if not package.is_file():
            raise ResourceImportError(f"resource package not found: {package}")
        package_size = package.stat().st_size
        archive_hash = sha256_file(package)
        if expected_sha256 and archive_hash.lower() != expected_sha256.lower():
            raise ResourceImportError("resource archive SHA-256 does not match the expected value")
        kind, name = self._classify(package)
        task_item = metadata or {"kind": kind, "name": name, "size": package.stat().st_size, "sha256": archive_hash}
        task_path = self._task_path(archive_hash)
        self._update_task(task_path, task_item, "importing", package.stat().st_size, package.stat().st_size)
        target = self.runtime_root if kind == "runtime" else self.model_root / name
        if target.exists() and not replace:
            if target.is_dir() and not any(target.iterdir()):
                pass
            else:
                if kind == "runtime":
                    validate_tsfm_runtime(target)
                else:
                    validate_tsfm_model(target, name, {})
                self._cleanup_cached_archive(package)
                self._update_task(task_path, task_item, "completed", package.stat().st_size, package.stat().st_size)
                return {"status": "unchanged", "kind": kind, "name": name, "path": str(target)}

        target.parent.mkdir(parents=True, exist_ok=True)
        staging = target.parent / f".{target.name}.tdgpt-stage-{uuid.uuid4().hex}"
        try:
            prefix = "model_venvs/tsfm" if kind == "runtime" else f"model/{name}"
            self._extract(package, prefix, staging)
            if kind == "runtime":
                self._relocate_runtime(staging)
                self._runtime_complete(staging, archive_hash)
            else:
                validate_tsfm_model(staging, name, {})
            self._activate(staging, target)
            if kind == "model":
                self._record_model_state(
                    metadata or {
                        "name": name,
                        "size": package.stat().st_size,
                    },
                    target,
                    archive_hash,
                    datetime.now(timezone.utc).isoformat(),
                )
            self._cleanup_cached_archive(package)
            self._update_task(task_path, task_item, "completed", package_size, package_size)
            return {"status": "imported", "kind": kind, "name": name, "path": str(target)}
        except Exception as exc:
            self._update_task(task_path, task_item, "failed", package_size, package_size, str(exc))
            raise
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    def import_packages(self, packages: Iterable[str], expected_sha256: str = "", replace: bool = False) -> List[Dict[str, Any]]:
        self._acquire_lock()
        try:
            return [self.import_package(package, expected_sha256, replace) for package in packages]
        finally:
            self._release_lock()

    def list_resources(self, target: str = "all") -> Dict[str, Any]:
        result: Dict[str, Any] = {"runtime": None, "models": [], "tasks": []}
        if self.task_root.is_dir():
            for path in sorted(self.task_root.glob("*.json")):
                try:
                    task = load_manifest(path)
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
                if target == "all" or task.get("name") == target:
                    result["tasks"].append(task)
        try:
            manifest, _ = validate_tsfm_runtime(self.runtime_root)
            result["runtime"] = manifest
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        if self.model_root.is_dir():
            names = sorted(path.name for path in self.model_root.iterdir() if path.is_dir())
            if target != "all":
                names = [name for name in names if name == target]
            for name in names:
                path = self.model_root / name
                state = {}
                try:
                    state = load_manifest(self.state_root / f"{name}.json")
                except (OSError, ValueError, json.JSONDecodeError):
                    pass
                try:
                    validate_tsfm_model(path, name, {})
                    install_status = "installed"
                except (OSError, ValueError):
                    install_status = "invalid"
                digest = state.get("archive_sha256", "")
                task = next((entry for entry in reversed(result["tasks"]) if entry.get("name") == name), {})
                model_entry = {
                    "name": name,
                    "model_id": state.get("model_id"),
                    "model_version": state.get("model_version"),
                    "version": state.get("resource_version"),
                    "digest": f"sha256:{digest}" if digest else None,
                    "size": state.get("size"),
                    "deployed_at": state.get("deployed_at"),
                    "install_status": install_status,
                    "note": state.get("note", ""),
                    "path": str(path),
                }
                if task.get("status") in {"downloading", "verifying", "importing", "failed"}:
                    model_entry.update({
                        "operation_status": task.get("status"),
                        "bytes_done": task.get("bytes_done"),
                        "bytes_total": task.get("bytes_total"),
                    })
                result["models"].append(model_entry)
        known = {item["name"] for item in result["models"]}
        for task in result["tasks"]:
            name = task.get("name")
            if task.get("kind") == "model" and name not in known and task.get("status") not in {"completed", "downloaded"}:
                result["models"].append({
                    "name": name,
                    "model_id": task.get("model_id"),
                    "model_version": task.get("model_version"),
                    "version": task.get("version"),
                    "digest": task.get("digest"),
                    "size": task.get("size"),
                    "deployed_at": None,
                    "install_status": "missing",
                    "operation_status": task.get("status"),
                    "bytes_done": task.get("bytes_done"),
                    "bytes_total": task.get("bytes_total"),
                    "note": task.get("error", ""),
                    "path": str(self.model_root / name),
                })
        return result
