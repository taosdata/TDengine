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

import ast
import hashlib
import importlib.util
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("arch", ["x64", "arm64"])
@pytest.mark.parametrize("venv_version", [None, "1.0.9"])
def test_lite_release_builds_only_runtime_main_venv_and_installer(tmp_path, arch, venv_version):
    packaging = tmp_path / "packaging"
    packaging.mkdir()
    source = Path(__file__).resolve().parents[1] / "packaging" / "build_linux_offline_release.sh"
    driver = packaging / source.name
    shutil.copyfile(source, driver)
    builder = '''set -eu
output=""
venv_version="1.0.4"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-dir) output="$2"; shift 2 ;;
    --arch) arch="$2"; shift 2 ;;
    --venv-version) venv_version="$2"; shift 2 ;;
    --bundle) test "$2" = main; shift 2 ;;
    --tdmodel-requirements) exit 99 ;;
    --lite) shift ;;
    *) shift 2 ;;
  esac
done
printf payload > "$output/ARTIFACT-linux-$arch.tar.xz"
'''
    for filename, artifact in [
        ("build_python_runtime_linux.sh", "tdengine-python-runtime-3.12.16"),
        ("build_linux_offline_assets.sh", "tdengine-tdgpt-lite-venv-${venv_version}"),
    ]:
        stub = packaging / filename
        stub.write_text(builder.replace("ARTIFACT", artifact))
        stub.chmod(0o755)
    script = tmp_path / "script"
    script.mkdir()
    release_sh = script / "release.sh"
    release_sh.write_text(
        'set -eu\ntest "$5" = -l\nmkdir -p ../release\n'
        f'printf installer > ../release/tdengine-tdgpt-enterprise-lite-3.4.2.9.0909-linux-{arch}.tar.xz\n'
    )
    release_sh.chmod(0o755)
    result = subprocess.run(
        ["bash", str(driver), "--lite", "--version", "3.4.2.9.0909",
         "--arch", arch, "--work-dir", "work dir", "--output-dir", "output dir",
         *(["--venv-version", venv_version] if venv_version else [])],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    output = tmp_path / "output dir"
    expected_venv_version = venv_version or "1.0.4"
    assert (output / f"tdengine-tdgpt-lite-venv-{expected_venv_version}-linux-{arch}.tar.xz").is_file()
    assert len(list(output.glob("*.tar.xz"))) == 3
    assert len((output / "SHA256SUMS").read_text().splitlines()) == 3
    for line in (output / "SHA256SUMS").read_text().splitlines():
        expected, filename = line.split(maxsplit=1)
        digest = hashlib.sha256((output / filename).read_bytes()).hexdigest()
        assert digest == expected, filename


def test_lite_release_rejects_tdmodel_requirements(tmp_path):
    script = Path(__file__).resolve().parents[1] / "packaging" / "build_linux_offline_release.sh"

    result = subprocess.run(
        [
            "bash",
            str(script),
            "--lite",
            "--version",
            "3.4.2.9.0909",
            "--tdmodel-requirements",
            str(tmp_path / "requirements.txt"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "cannot be used with --lite" in result.stderr


def test_docker_runtime_rejects_unknown_architecture(tmp_path):
    script = Path(__file__).resolve().parents[1] / "packaging" / "build_python_runtime_linux_docker.sh"
    result = subprocess.run(
        ["bash", str(script), "--arch", "x86_64"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert (
        "must be x64 or arm64" in result.stderr
        or "must run on Linux" in result.stderr
    )


@pytest.mark.parametrize("image,expected", [
    ("registry:5000/team/runtime", "registry:5000/team/runtime:3.12.16-amd64"),
    ("registry:5000/team/runtime:uv", "registry:5000/team/runtime:uv-3.12.16-amd64"),
])
@pytest.mark.parametrize("deny_mount", [False, True])
def test_docker_runtime_probes_mounts_before_building_runtime(tmp_path, image, expected, deny_mount):
    script = Path(__file__).resolve().parents[1] / "packaging" / "build_python_runtime_linux_docker.sh"
    tools = tmp_path / "tools"
    tools.mkdir()
    for name, body in {
        "uname": 'case "$1" in -s) echo Linux;; -m) echo x86_64;; esac',
        "docker": '''printf '%s\\n' "$*" >> "$CALL_LOG"
case "$*" in
  *tdgpt-write-probe*) test "$DENY_MOUNT" = 0 || { echo 'Container cannot write /out' >&2; exit 1; };;
esac''',
    }.items():
        stub = tools / name
        stub.write_text("#!/bin/sh\n" + body + "\n")
        stub.chmod(0o755)
    log = tmp_path / "calls"
    result = subprocess.run(
        ["bash", str(script), "--image", image, "--work-dir", str(tmp_path / "work space"),
         "--output-dir", str(tmp_path / "out space")],
        env={**os.environ, "PATH": f"{tools}:{os.environ['PATH']}",
             "CALL_LOG": str(log), "DENY_MOUNT": str(int(deny_mount))},
        capture_output=True, text=True,
    )
    calls = log.read_text()
    assert f"-t {expected}" in calls
    assert "tdgpt-write-probe" in calls
    if deny_mount:
        assert result.returncode != 0
        assert "Container cannot write /out" in result.stderr
        assert "--work-dir /work" not in calls
    else:
        assert result.returncode == 0, result.stderr
        assert calls.index("tdgpt-write-probe") < calls.index("--work-dir /work")


def test_runtime_docker_copy_inputs_are_in_context():
    packaging = Path(__file__).resolve().parents[1] / "packaging"
    allowlist = (packaging / ".dockerignore").read_text().splitlines()
    for line in (packaging / "Dockerfile.python-runtime").read_text().splitlines():
        if line.startswith("COPY "):
            source = line.split()[1]
            assert (packaging / source).is_file()
            assert f"!{source}" in allowlist, f"Docker COPY source excluded: {source}"


def load_tsfm_builder():
    source = Path(__file__).resolve().parents[1] / "offline-pkgs" / "build_tsfm_runtime.py"
    spec = importlib.util.spec_from_file_location("tsfm_builder", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("output,code,stderr,message", [
    ("uv 0.13.9", 0, "", None),
    ("uv 0.14.0 (build)", 0, "", None),
    ("uv 0.13.8", 0, "", "uv >= 0.13.9"),
    ("unknown", 0, "", "unable to parse"),
    ("", 1, "broken executable", "broken executable"),
])
def test_tsfm_uv_preflight(monkeypatch, output, code, stderr, message):
    module = load_tsfm_builder()
    monkeypatch.setattr(module.shutil, "which", lambda _: "/tools/uv")
    monkeypatch.setattr(module.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, code, output, stderr))
    if message:
        with pytest.raises(RuntimeError, match=message):
            module.resolve_uv("uv")
    else:
        assert module.resolve_uv("uv") == "/tools/uv"


def test_arm_tsfm_cpu_check_rejects_missing_lib_and_cuda(tmp_path, monkeypatch):
    module = load_tsfm_builder()
    monkeypatch.setattr(module.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, "", ""))
    with pytest.raises(RuntimeError, match="library directory not found"):
        module.validate_cpu_torch(Path("python"), tmp_path, "arm64")
    lib = tmp_path / "lib"
    lib.mkdir()
    module.validate_cpu_torch(Path("python"), tmp_path, "arm64")
    (lib / "libtorch_cuda.so").touch()
    with pytest.raises(RuntimeError, match="CUDA torch library"):
        module.validate_cpu_torch(Path("python"), tmp_path, "arm64")


@pytest.mark.parametrize("output,code,message", [
    ("unexpected version format", 0, "Unable to parse uv version"),
    ("uv failed to start", 1, "Unable to query uv version"),
    ("uv 0.13.8", 0, "uv >= 0.13.9"),
])
def test_linux_runtime_uv_errors_include_original_output(tmp_path, output, code, message):
    script = Path(__file__).resolve().parents[1] / "packaging" / "build_python_runtime_linux.sh"
    uv = tmp_path / "uv executable"
    uv.write_text(f"#!/bin/sh\necho '{output}'\nexit {code}\n")
    uv.chmod(0o755)
    result = subprocess.run(["bash", str(script), "--uv-exe", str(uv)], capture_output=True, text=True)
    assert result.returncode != 0
    assert message in result.stderr
    assert output.removeprefix("uv ") in result.stderr


@pytest.mark.parametrize("pip_args", [[], ["--index-url", "https://example.invalid/simple"]])
@pytest.mark.parametrize("extra_requirements", [[], ["https://example.invalid/compatible.whl#sha256=abc"]])
def test_linux_assets_create_venv_preserves_optional_pip_arguments(tmp_path, pip_args, extra_requirements):
    source = (Path(__file__).resolve().parents[1] / "packaging" / "build_linux_offline_assets.sh").read_text()
    create_venv = source[source.index("create_venv() {"):source.index("prune_venv() {")]
    runtime = tmp_path / "python"
    runtime.write_text('''#!/bin/bash
mkdir -p "$3/bin"
cat > "$3/bin/pip3" <<'EOF'
#!/bin/bash
printf '<%s>' "$@" >> "$CALL_LOG"
printf '\\n' >> "$CALL_LOG"
EOF
chmod +x "$3/bin/pip3"
''')
    runtime.chmod(0o755)
    log = tmp_path / "calls"
    driver = tmp_path / "driver.sh"
    driver.write_text(
        'set -eu\nbuild_python="$1"\nvenv="$2"\nshift 2\npip_args=("$@")\n'
        + create_venv + '\ncreate_venv "requirements file" "$venv" '
        + " ".join(extra_requirements) + '\n'
    )
    result = subprocess.run(["bash", str(driver), str(runtime), str(tmp_path / "venv"), *pip_args],
                            env={**os.environ, "CALL_LOG": str(log)}, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    expected = "<install>" + "".join(f"<{argument}>" for argument in pip_args + extra_requirements) + "<-r><requirements file>"
    assert log.read_text().splitlines()[1] == expected


@pytest.mark.parametrize("arch, wheel_arch", [("x64", "x86_64"), ("arm64", "aarch64")])
def test_lite_pyarrow_wheel_keeps_manylinux2014_baseline(arch, wheel_arch):
    source = (Path(__file__).resolve().parents[1] / "packaging" / "build_linux_offline_assets.sh").read_text()
    function = source[source.index("lite_pyarrow_wheel() {"):source.index("create_venv() {")]
    result = subprocess.run(["bash", "-eu", "-c", f'arch={arch}\n' + function + '\nlite_pyarrow_wheel'],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    wheel, digest = result.stdout.strip().split("#sha256=")
    assert wheel.endswith(f"pyarrow-18.1.0-cp312-cp312-manylinux_2_17_{wheel_arch}.manylinux2014_{wheel_arch}.whl")
    assert len(digest) == 64
    assert all(character in "0123456789abcdef" for character in digest)


@pytest.mark.parametrize("resolver,option", [
    ("resolve_python_bin", "python_home"),
    ("resolve_repair_python_bin", "python_runtime_dir"),
])
@pytest.mark.parametrize("runtime_state", ["omitted", "valid", "missing"])
def test_explicit_python_runtime_never_falls_back_to_system(tmp_path, resolver, option, runtime_state):
    source = (Path(__file__).resolve().parents[1] / "script" / "install.sh").read_text()
    functions = "\n".join(
        re.search(rf"function {name}\(\) \{{.*?^\}}", source, re.MULTILINE | re.DOTALL).group()
        for name in ("resolve_python_bin", "resolve_repair_python_bin")
    )
    runtime = tmp_path / "explicit runtime"
    (runtime / "bin").mkdir(parents=True)
    if runtime_state == "valid":
        python = runtime / "bin" / "python3.12"
        python.write_text("#!/bin/sh\nexit 0\n")
        python.chmod(0o755)
    system_bin = tmp_path / "system"
    system_bin.mkdir()
    system_python = system_bin / "python3.12"
    system_python.write_text("#!/bin/sh\nexit 0\n")
    system_python.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", functions + "\n" + resolver],
        env={**os.environ, "PATH": f"{system_bin}:{os.environ['PATH']}",
             "python_minor_ver": "12", "python_home": "", "python_runtime_dir": "",
             option: str(runtime) if runtime_state != "omitted" else ""},
        capture_output=True, text=True,
    )
    if runtime_state == "missing":
        assert result.returncode != 0
        assert "No executable Python found in explicit" in result.stderr
        assert result.stdout == ""
    else:
        assert result.returncode == 0, result.stderr
        expected = runtime / "bin" / "python3.12" if runtime_state == "valid" else system_python
        assert result.stdout.strip() == str(expected)


def test_lite_venv_default_version_contract():
    """Keep native build entry points and their help text on the same release."""
    packaging = Path(__file__).resolve().parents[1] / "packaging"
    expected = "1.0.4"
    for name in ("build_linux_offline_release.sh", "build_linux_offline_assets.sh"):
        source = (packaging / name).read_text(encoding="utf-8")
        version = re.search(r'^venv_version="([^"]+)"$', source, re.MULTILINE).group(1)
        help_version = re.search(r"--venv-version VERSION[^\n]*Default: ([0-9.]+)\.", source).group(1)
        assert version == expected, name
        assert help_version == version, name
    tree = ast.parse((packaging / "build_windows_lite_venv.py").read_text(encoding="utf-8"))
    version_default = next(
        keyword.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "add_argument"
        and any(isinstance(arg, ast.Constant) and arg.value == "--version" for arg in node.args)
        for keyword in node.keywords if keyword.arg == "default"
    )
    assert ast.literal_eval(version_default) == expected
