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

"""Persistent algorithm paths and upgrade migration (standard library only)."""

import argparse
import errno
import hashlib
import importlib
import importlib.machinery
import importlib.util
import json
import os
import runpy
import shutil
import sys
import tempfile
import types
import uuid
from pathlib import Path


def custom_directory(config, default, install_dir, *, use_environment=True):
    """Resolve one shared installer/loader path, without creating runtime data."""
    if "data_dir" in config:
        default = Path(config["data_dir"]) / "custom"
    override = os.environ.get("TDGPT_CUSTOM_ALGO_DIR") if use_environment else None
    value = override or config.get("custom_algo_dir", default)
    if not isinstance(value, (str, os.PathLike)) or not str(value).strip():
        raise ValueError("custom_algo_dir must be a non-empty absolute directory")
    path = Path(value)
    if not path.is_absolute():
        raise ValueError("custom_algo_dir must be an absolute directory")
    path = path.resolve()
    install = Path(install_dir).resolve()
    protected = [install / name for name in (
        "bin", "lib", "requirements", "python", "resource", "log",
        "venv", "venvs", "model", "model_venvs",
    )]
    for key in ("model_dir", "dynamic_model_dir", "model_venvs_dir", "tsfm_venv", "virtualenv", "resource_dir"):
        if config.get(key):
            protected.append(Path(config[key]).resolve())
    protected += [Path(default).resolve().parent / name for name in ("model", "model_venvs", "venv", "resource")]
    for other in protected:
        if path == other or other in path.parents or path in other.parents:
            raise ValueError(f"custom_algo_dir overlaps managed directory: {other}")
    # Linux installers clean the program root, preserving only data/log/cfg.
    if os.name != "nt" and install in path.parents and install / "data" not in path.parents:
        raise ValueError("custom_algo_dir inside the Linux program tree must be under data/")
    return path


def module_prefix(root):
    """Create an isolated package namespace supporting relative plugin imports."""
    root = Path(root).resolve()
    name = "_tdgpt_custom_" + hashlib.sha256(os.fsencode(root)).hexdigest()[:16]
    if name not in sys.modules:
        initializer = root / "__init__.py"
        if initializer.is_file():
            spec = importlib.util.spec_from_file_location(
                name, initializer, submodule_search_locations=[str(root)],
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            try:
                spec.loader.exec_module(module)
            except BaseException:
                # Discard partially imported helpers so a later retry starts cleanly.
                for key in list(sys.modules):
                    if key == name or key.startswith(name + "."):
                        del sys.modules[key]
                raise
        else:
            module = types.ModuleType(name)
            module.__path__ = [str(root)]
            module.__package__ = name
            module.__spec__ = importlib.machinery.ModuleSpec(name, loader=None, is_package=True)
            module.__spec__.submodule_search_locations = module.__path__
            sys.modules[name] = module
    importlib.invalidate_caches()
    return name


def _files(root):
    if not root.exists():
        return []
    if root.is_symlink():
        raise ValueError(f"legacy custom algorithm symlink needs manual migration: {root}")
    result = []
    for path in sorted(root.rglob("*")):
        if "__pycache__" in path.parts or path.suffix in (".pyc", ".pyo"):
            continue
        if path.is_symlink():
            raise ValueError(f"legacy custom algorithm symlink needs manual migration: {path}")
        if path.is_file():
            result.append(path)
    return result


def migrate(install_dir, default, config_path, backup_dir, *, check_only=False):
    """Copy before program replacement; refuse all conflicts before writing files.

    The existing config is trusted executable administrator configuration, just
    as it is when the service loads it. Never import the analytics dependency tree.
    """
    install = Path(install_dir).resolve()
    config_path = Path(config_path)
    config = runpy.run_path(str(config_path)) if config_path.is_file() else {}
    # Installer environments are transient; migrate to the persisted location
    # that the service will use after the installer exits.
    target = custom_directory(config, default, install, use_environment=False)
    legacy = install / "lib" / "taosanalytics" / "algo" / "custom"
    files = _files(legacy)
    pending = []
    conflicts = set()
    for source in files:
        dest = target / source.relative_to(legacy)
        for parent in dest.parents:
            if parent == target:
                break
            if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
                conflicts.add(str(parent))
        if dest.is_symlink() or (dest.exists() and (not dest.is_file() or source.read_bytes() != dest.read_bytes())):
            conflicts.add(f"{source} -> {dest}")
        if not dest.exists():
            pending.append((source, dest))
    for path in (target, target / "ad", target / "fc"):
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            conflicts.add(str(path))
    if conflicts:
        raise FileExistsError("custom algorithm migration conflicts:\n" + "\n".join(sorted(conflicts)))
    if check_only:
        return {"directory": str(target), "pending": len(pending)}
    backup = None
    if legacy.exists():
        backup = Path(backup_dir) / ("custom-" + uuid.uuid4().hex)
        shutil.copytree(legacy, backup, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"))
    target.mkdir(parents=True, exist_ok=True)
    for kind in ("ad", "fc"):
        (target / kind).mkdir(exist_ok=True)
    for source, dest in pending:
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Finish the copy before exposing the destination; link refuses a
        # concurrent administrator edit rather than overwriting it.
        with tempfile.NamedTemporaryFile(dir=dest.parent, delete=False) as out:
            temp = Path(out.name)
        try:
            shutil.copy2(source, temp)
            if source.read_bytes() != temp.read_bytes():
                raise OSError(f"custom algorithm migration verification failed: {dest}")
            _publish_copy(temp, dest)
        finally:
            temp.unlink(missing_ok=True)
    # Old configs without data_dir need the installer's actual data root (not a
    # fixed algorithm path), so future data_dir edits also move the default.
    if config_path.is_file() and "data_dir" not in config and "custom_algo_dir" not in config:
        original = config_path.read_bytes()
        # ASCII escapes preserve non-UTF-8 source encodings and any original BOM.
        updated = original + ("\n# Persistent application data.\ndata_dir = " + ascii(str(Path(default).parent)) + "\n").encode("ascii")
        compile(updated, str(config_path), "exec")
        with tempfile.NamedTemporaryFile(dir=config_path.parent, delete=False) as out:
            temp = Path(out.name)
            out.write(updated)
        try:
            shutil.copystat(config_path, temp)
            os.replace(temp, config_path)
        finally:
            temp.unlink(missing_ok=True)
    record_path = None
    if backup is not None:
        # Verify even pre-existing matching destinations before retiring the only
        # legacy source. An interrupted copy leaves the source available to retry.
        digests = {}
        if _files(legacy) != files:
            raise OSError("legacy custom algorithms changed during migration")
        for source in files:
            relative = source.relative_to(legacy)
            contents = source.read_bytes()
            if contents != (target / relative).read_bytes() or contents != (backup / relative).read_bytes():
                raise OSError(f"custom algorithm migration verification failed: {source}")
            digests[relative.as_posix()] = hashlib.sha256(contents).hexdigest()
        retired = legacy.with_name(".custom-retired-" + uuid.uuid4().hex)
        record_path = backup.with_suffix(".migration.json")
        record = {
            "source": str(legacy), "directory": str(target), "backup": str(backup),
            "files": digests, "state": "verified",
        }
        _write_migration_record(record_path, record)
        # Rename on the source filesystem so a partial cleanup can never expose
        # stale plugins at the legacy path on the next Windows upgrade.
        legacy.rename(retired)
        record["state"] = "retired"
        _write_migration_record(record_path, record)
        shutil.rmtree(retired)
    return {
        "directory": str(target), "copied": len(pending),
        "backup": str(backup) if backup else None,
        "record": str(record_path) if record_path else None,
    }


def _publish_copy(temp, dest):
    """Publish without overwriting; support volumes without hard links."""
    try:
        os.link(temp, dest)
        return
    except OSError as exc:
        if (exc.errno not in (errno.EPERM, errno.EACCES, errno.ENOTSUP, errno.EXDEV, errno.ENOSYS)
                and getattr(exc, "winerror", None) not in (1, 50, 1314)):
            raise
    # Exclusive creation also refuses concurrent edits. A partial copy is
    # removed on failure so the original legacy tree remains retryable.
    descriptor = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as out:
        try:
            with temp.open("rb") as source:
                shutil.copyfileobj(source, out)
            out.flush()
            shutil.copystat(temp, dest)
        except BaseException:
            out.close()
            dest.unlink()
            raise


def _write_migration_record(path, record):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as out:
        temp = Path(out.name)
        out.write((json.dumps(record, indent=2) + "\n").encode("utf-8"))
    try:
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install-dir", required=True)
    parser.add_argument("--default", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--backup-dir", required=True)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(migrate(args.install_dir, args.default, args.config, args.backup_dir, check_only=args.check_only)))
