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

"""Enable and archive only cores belonging to this CTest invocation."""

import json
import os
import pwd
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys
import tempfile
import time


def remove_owned_core(path):
    # /cores may grant add_file without delete_child. Grant deletion on our
    # own file rather than broadening permissions on the shared directory.
    if path.stat().st_uid != os.getuid():
        raise PermissionError(f"Refusing to delete another user's core: {path}")
    try:
        path.unlink()
    except PermissionError:
        subprocess.run(["chmod", "+a", f"user:{pwd.getpwuid(os.getuid()).pw_name} allow delete", str(path)], check=True)
        path.unlink()


def prepare(debug_dir, artifacts):
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "core-state.json").unlink(missing_ok=True)
    (artifacts / "core-archives.json").unlink(missing_ok=True)
    try:
        prepare_enabled(debug_dir, artifacts)
    except Exception as error:
        (artifacts / "core-state.json").unlink(missing_ok=True)
        write_json(artifacts / "core-setup.json", {"enabled": False, "error": str(error)})
        raise


def write_json(path, value):
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, indent=2)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def prepare_enabled(debug_dir, artifacts):
    listing = subprocess.check_output(
        ["ctest", "--test-dir", str(debug_dir), "--show-only=json-v1"], text=True)
    binaries = sorted({str(Path(test["command"][0]).resolve())
                       for test in json.loads(listing)["tests"] if test.get("command")})
    binaries = [path for path in binaries if Path(path).is_file()
                and debug_dir.resolve() in Path(path).parents]
    core_pattern = subprocess.check_output(["sysctl", "-n", "kern.corefile"], text=True).strip()
    core_dir = Path(core_pattern).parent
    if not core_dir.is_absolute() or "%" in str(core_dir) or Path(core_pattern).name != "core.%P":
        raise RuntimeError(f"Unsupported macOS core directory: {core_pattern}")
    if not binaries:
        raise RuntimeError("No CTest executables found in the build directory")
    if subprocess.check_output(["sysctl", "-n", "kern.coredump"], text=True).strip() != "1":
        raise RuntimeError("kern.coredump must be enabled on the runner")
    # access(W_OK) does not reliably reflect add_file ACLs on macOS.
    fd, name = tempfile.mkstemp(prefix=".tsdb-core-write-check-", dir=core_dir)
    os.close(fd)
    remove_owned_core(Path(name))
    entitlements = artifacts / "core-entitlements.plist"
    entitlements.write_text('<?xml version="1.0"?><!DOCTYPE plist PUBLIC '
                            '"-//Apple//DTD PLIST 1.0//EN" '
                            '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">'
                            '<plist version="1.0"><dict><key>com.apple.security.get-task-allow</key>'
                            '<true/></dict></plist>')
    # On Apple Silicon, unlimited RLIMIT_CORE alone is insufficient. Only
    # CTest executables receive this debug entitlement, not installed products.
    for binary in binaries:
        subprocess.run(["codesign", "--force", "--sign", "-", "--entitlements",
                        str(entitlements), binary], check=True)
    uuids = {}
    for binary in binaries:
        identity = subprocess.check_output(["dwarfdump", "--uuid", binary], text=True)
        for uuid in re.findall(r"UUID: ([0-9A-Fa-f-]{36})", identity):
            uuids[uuid.upper()] = binary
    _, hard = resource.getrlimit(resource.RLIMIT_CORE)
    resource.setrlimit(resource.RLIMIT_CORE, (hard, hard))
    if hard == 0:
        raise RuntimeError("Runner hard core limit is zero")
    job = os.environ.get("CI_JOB_ID", str(time.time_ns()))
    if not job.isdigit():
        raise RuntimeError(f"Invalid CI_JOB_ID: {job}")
    archive_dir = debug_dir.resolve().parent.parent / "unit-cores" / f"job-{job}"
    state = {"archive_dir": str(archive_dir), "debug_dir": str(debug_dir.resolve()), "started": time.time(), "core_dir": str(core_dir), "binaries": binaries, "uuids": uuids,
             "existing": [str(p) for p in core_dir.iterdir()],
             "commit": os.environ.get("CI_COMMIT_SHA"), "job_id": os.environ.get("CI_JOB_ID")}
    write_json(artifacts / "core-state.json", state)
    write_json(artifacts / "core-setup.json", {"enabled": True})
    print(f"[macos-ci] core dumps enabled for {len(binaries)} CTest executables: {core_pattern}", flush=True)


def retain_debug_bundle(executable, output, core_name):
    retained = output / (core_name + ".executable")
    symbols = output / (core_name + ".dSYM")
    shutil.copy2(executable, retained)
    retained.chmod(0o600)
    result = subprocess.run(["dsymutil", str(executable), "-o", str(symbols)],
                            check=True, timeout=120, capture_output=True, text=True)
    if "warning:" in result.stderr.lower():
        raise RuntimeError(f"dsymutil could not retain complete symbols: {result.stderr.strip()}")
    if not (symbols / "Contents/Resources/DWARF").is_dir():
        raise RuntimeError(f"debug symbols were not generated: {symbols}")
    executable_identity = subprocess.check_output(["dwarfdump", "--uuid", str(retained)], text=True, timeout=30)
    symbol_identity = subprocess.check_output(["dwarfdump", "--uuid", str(symbols)], text=True, timeout=30)
    executable_uuids = set(re.findall(r"UUID: ([0-9A-Fa-f-]{36})", executable_identity.upper()))
    symbol_uuids = set(re.findall(r"UUID: ([0-9A-Fa-f-]{36})", symbol_identity.upper()))
    if not executable_uuids or executable_uuids != symbol_uuids:
        raise RuntimeError("dSYM UUIDs do not match the retained executable")
    debug_info = subprocess.check_output(["dwarfdump", "--debug-info", str(symbols)], text=True, timeout=60)
    if "DW_TAG_compile_unit" not in debug_info or "DW_TAG_subprogram" not in debug_info:
        raise RuntimeError("dSYM has no usable DWARF function information")
    return {"executable": str(retained), "symbols": str(symbols)}


def collect(artifacts):
    state_path = artifacts / "core-state.json"
    if not state_path.exists():
        print("[macos-ci] no core state: CTest core setup did not complete", flush=True)
        return
    state = json.loads(state_path.read_text())
    preserve = artifacts / "core-preserve-build"
    preserve.touch()
    # Keep large cores outside the runner checkout. The next git clean and
    # GitLab artifact size limits must not discard the only crash evidence.
    output = Path(state["archive_dir"])
    if output.exists():
        (output / "completed.json").unlink(missing_ok=True)
    index = artifacts / "core-archives.json"
    archives = json.loads(index.read_text()) if index.exists() else []
    saved = 0
    failed = any(archive.get("symbols_error") for archive in archives)
    for core in sorted(Path(state["core_dir"]).iterdir()):
        if not core.name.startswith("core."):
            continue
        try:
            if (str(core) in state["existing"] or not core.is_file() or core.is_symlink()
                    or core.stat().st_uid != os.getuid() or core.stat().st_mtime < state["started"]):
                continue
            result = subprocess.run(["lldb", "--batch", "--core", str(core), "-o", "image list -u -f"],
                                    capture_output=True, text=True, timeout=60)
            main_image = re.search(r"(?m)^\[\s*0\] (.+)$", result.stdout)
            if result.returncode or not main_image:
                raise RuntimeError("LLDB could not identify the main image")
            # CTest-spawned processes may omit the executable path from the core.
            # Match the main image UUID against the exact CTest Mach-O binaries.
            image = main_image.group(1).strip()
            uuid = re.match(r"([0-9A-Fa-f-]{36})(?: |$)", image)
            if not ((uuid and uuid.group(1).upper() in state.get("uuids", {}))
                    or image in state["binaries"]):
                continue
            executable = (state.get("uuids", {}).get(uuid.group(1).upper())
                          if uuid else image)
            output.mkdir(parents=True, exist_ok=True)
            target = output / core.name
            descriptor, temporary_name = tempfile.mkstemp(prefix=f".{core.name}.", suffix=".partial", dir=output)
            temporary = Path(temporary_name)
            try:
                with os.fdopen(descriptor, "wb") as raw, core.open("rb") as src:
                    shutil.copyfileobj(src, raw, length=1024 * 1024)
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
            (output / (core.name + ".images.txt")).write_text(result.stdout)
            bundle = {"original_executable": executable}
            try:
                bundle.update(retain_debug_bundle(Path(executable), output, core.name))
            except Exception as error:
                failed = True
                bundle["symbols_error"] = str(error)
                print(f"[macos-ci] WARNING: symbols unavailable for {core}: {error}", flush=True)
            archives = [archive for archive in archives if archive["path"] != str(target)]
            archives.append({"path": str(target), "size": target.stat().st_size, **bundle})
            write_json(output / (core.name + ".json"), {
                **archives[-1], "commit": state.get("commit"), "job_id": state.get("job_id"),
                "uuids": state.get("uuids", {})})
            write_json(artifacts / "core-archives.json", archives)
            saved += 1
            print(f"[macos-ci] saved unit-test core: {target} ({target.stat().st_size} bytes)", flush=True)
            remove_owned_core(core)
        except Exception as error:
            failed = True
            print(f"[macos-ci] WARNING: failed to archive {core}: {error}", flush=True)
    write_json(artifacts / "core-archives.json", archives)
    if not failed:
        preserve.unlink(missing_ok=True)
    print(f"[macos-ci] archived {saved} unit-test core(s); unrelated cores were left untouched", flush=True)


def finish_collection(artifacts):
    state_path = artifacts / "core-state.json"
    if not state_path.exists():
        return
    state = json.loads(state_path.read_text())
    output = Path(state["archive_dir"])
    if (artifacts / "core-preserve-build").exists():
        debug_dir = Path(state["debug_dir"])
        output.mkdir(parents=True, exist_ok=True)
        destination = output / "debug"
        if debug_dir.exists():
            if destination.exists():
                raise RuntimeError(f"Refusing to overwrite preserved build: {destination}")
            debug_dir.rename(destination)
        elif not destination.is_dir():
            raise RuntimeError(f"Build evidence is missing: {debug_dir}")
        write_json(artifacts / "core-preserved-build.json", {"path": str(destination), "original_path": str(debug_dir)})
        print(f"[macos-ci] preserved incomplete diagnostic build: {destination}", flush=True)
    if output.exists():
        write_json(output / "completed.json", {"finished": time.time(), "job_id": state.get("job_id")})


def cleanup_archives(root, retention_days, dry_run=False):
    if retention_days < 1:
        raise ValueError("retention days must be positive")
    cutoff = time.time() - retention_days * 24 * 60 * 60
    removed = 0
    root = Path(root)
    if not root.is_dir():
        return
    for archive in root.glob("job-*"):
        try:
            if (archive.is_symlink() or not archive.is_dir() or not archive.name[4:].isdigit()
                    or archive.stat().st_uid != os.getuid() or (archive / "KEEP").exists()
                    or any(archive.glob("*.partial")) or any(archive.glob(".*.partial"))
                    or not (archive / "completed.json").is_file()):
                continue
            finished = json.loads((archive / "completed.json").read_text())["finished"]
            if finished >= cutoff:
                continue
            print(f"[macos-ci] {'would remove' if dry_run else 'removing'} expired archive: {archive}", flush=True)
            if not dry_run:
                shutil.rmtree(archive)
            removed += 1
        except Exception as error:
            print(f"[macos-ci] WARNING: archive cleanup failed for {archive}: {error}", flush=True)
    print(f"[macos-ci] {'would remove' if dry_run else 'removed'} {removed} expired core archive(s)", flush=True)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--collect":
        collect(Path(sys.argv[2]).resolve())
    elif len(sys.argv) == 3 and sys.argv[1] == "--finish":
        finish_collection(Path(sys.argv[2]).resolve())
    elif len(sys.argv) in (5, 6) and sys.argv[1] == "--cleanup-archives" and sys.argv[3] == "--retention-days":
        if len(sys.argv) == 6 and sys.argv[5] != "--dry-run":
            raise SystemExit("unknown cleanup argument")
        cleanup_archives(Path(sys.argv[2]).resolve(), int(sys.argv[4]), dry_run=len(sys.argv) == 6)
    else:
        raise SystemExit("usage: macos_core_dumps.py --collect ARTIFACT_DIR | --finish ARTIFACT_DIR | --cleanup-archives ROOT --retention-days DAYS")
