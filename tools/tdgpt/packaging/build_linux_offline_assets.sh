#!/usr/bin/env bash
# Build TDgpt Linux offline venv archives on their native architecture.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
tdgpt_root=$(cd "${script_dir}/.." && pwd)
build_python_home=""
target_python_home="/usr/local/taos-deps/python-runtime"
tdmodel_requirements=""
work_dir="${PWD}/tdgpt-offline-assets-build"
output_dir="${PWD}/dist"
arch=""
pip_extra_args=""
bundle="all"
patchelf_exe="patchelf"
lite=0
venv_version="1.0.1"

usage() {
  cat <<'EOF'
Usage: build_linux_offline_assets.sh --build-python-home DIR [options]

Required:
  --build-python-home DIR      Existing CPython runtime used while building venvs.
  --tdmodel-requirements FILE  TDModel requirements.txt. Required only for --bundle all.

Options:
  --tdgpt-root DIR             TDgpt source root. Default: inferred from this script.
  --target-python-home DIR     Runtime path written into output venvs.
                               Default: /usr/local/taos-deps/python-runtime
  --work-dir DIR               Temporary workspace. Default: ./tdgpt-offline-assets-build
  --output-dir DIR             Archive output directory. Default: ./dist
  --arch x64|arm64             Validate native architecture and name outputs.
  --bundle all|main|model      Build all venvs (default), only the TDgpt main venv,
                               or only the complete model venv.
  --lite                       Build Lite main venv and use Lite archive names.
  --venv-version VERSION      Version used in venv archive names. Default: 1.0.1.
  --patchelf PATH              patchelf executable for the ARM64 Moirai fix.
                               Version 0.14.0 or newer is required.
  --pip-extra-args STRING      Extra pip arguments, such as --index-url <url>.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-python-home) build_python_home="$2"; shift 2 ;;
    --tdmodel-requirements) tdmodel_requirements="$2"; shift 2 ;;
    --tdgpt-root) tdgpt_root="$2"; shift 2 ;;
    --target-python-home) target_python_home="$2"; shift 2 ;;
    --work-dir) work_dir="$2"; shift 2 ;;
    --output-dir) output_dir="$2"; shift 2 ;;
    --arch) arch="$2"; shift 2 ;;
    --bundle) bundle="$2"; shift 2 ;;
    --lite) lite=1; shift ;;
    --venv-version) venv_version="$2"; shift 2 ;;
    --patchelf) patchelf_exe="$2"; shift 2 ;;
    --pip-extra-args) pip_extra_args="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$build_python_home" ]] || { echo "--build-python-home is required" >&2; exit 2; }
case "$bundle" in
  all|main|model) ;;
  *) echo "--bundle must be all, main, or model" >&2; exit 2 ;;
esac
[[ "$bundle" != "all" || -n "$tdmodel_requirements" ]] || {
  echo "--tdmodel-requirements is required for --bundle all" >&2
  exit 2
}
build_python_home=$(cd "$build_python_home" && pwd)
tdgpt_root=$(cd "$tdgpt_root" && pwd)
if [[ "$bundle" == "all" ]]; then
  tdmodel_requirements=$(cd "$(dirname "$tdmodel_requirements")" && pwd)/$(basename "$tdmodel_requirements")
fi
build_python="${build_python_home}/bin/python3"
[[ -x "$build_python" ]] || { echo "Missing runtime Python: $build_python" >&2; exit 1; }
if [[ "$bundle" == "all" ]]; then
  [[ -f "$tdmodel_requirements" ]] || { echo "Missing TDModel requirements: $tdmodel_requirements" >&2; exit 1; }
fi

case "$(uname -m)" in
  x86_64) native_arch=x64 ;;
  aarch64|arm64) native_arch=arm64 ;;
  *) echo "Unsupported Linux architecture: $(uname -m)" >&2; exit 1 ;;
esac
arch="${arch:-$native_arch}"
[[ "$arch" == "$native_arch" ]] || {
  echo "--arch $arch must be built on native $native_arch hardware" >&2
  exit 1
}

read -r -a pip_args <<< "$pip_extra_args"
mkdir -p "$work_dir" "$output_dir"
lock_file="${work_dir}/.lock-${arch}"
exec 9>"$lock_file"
flock -n 9 || { echo "Another ${arch} offline asset build is running" >&2; exit 1; }

cleanup_paths=()
cleanup() { rm -rf "${cleanup_paths[@]}"; }
trap cleanup EXIT

prepare_requirements() {
  local destination="$1"
  mkdir -p "$destination"
  cp "$tdgpt_root"/requirements_docker.txt "$tdgpt_root"/requirements_ess.txt \
    "$destination/"
  for optional_requirements in requirements_moirai.txt requirements_moment.txt; do
    if [[ -f "$tdgpt_root/$optional_requirements" ]]; then
      cp "$tdgpt_root/$optional_requirements" "$destination/"
    fi
  done

  # MomentFM 0.1.4 is installed separately with --no-deps after these pins.

  if [[ "$arch" == "arm64" ]]; then
    sed -i -E 's/(torch==[0-9.]+)\+cpu/\1/g' "$destination"/requirements_*.txt
    sed -i -E \
      -e '/^-f https:\/\/download\.pytorch\.org\/whl\/torch_stable\.html$/d' \
      -e '/^--find-links https:\/\/download\.pytorch\.org\/whl\/torch\/$/d' \
      "$destination"/requirements_*.txt
    # Avoid hf-xet's unsupported ARM CPython 3.12 build path.
    printf '\nhuggingface_hub==0.23.4\n' >> "$destination/requirements_ess.txt"
    printf '\nhuggingface_hub==0.23.4\npyarrow==16.1.0\n' >> "$destination/requirements_moirai.txt"
  fi
}

rewrite_runtime_refs() {
  local venv_dir="$1"
  local runtime_python="${target_python_home}/bin/python3"
  local cfg="${venv_dir}/pyvenv.cfg"
  [[ -f "$cfg" ]] || { echo "Missing pyvenv.cfg: $cfg" >&2; exit 1; }
  sed -i -E \
    -e "s|^home = .*|home = ${target_python_home}/bin|" \
    -e "s|^executable = .*|executable = ${runtime_python}|" \
    -e "s|^command = .*|command = ${runtime_python} -m venv ${venv_dir}|" \
    "$cfg"
  rm -f "${venv_dir}/bin/python" "${venv_dir}/bin/python3"
  ln -s "$runtime_python" "${venv_dir}/bin/python3"
  ln -s python3 "${venv_dir}/bin/python"
}

create_venv() {
  local requirements="$1"
  local venv_dir="$2"
  "$build_python" -m venv "$venv_dir"
  "${venv_dir}/bin/pip3" install --upgrade pip setuptools wheel
  PATH="${venv_dir}/bin:${PATH}" "${venv_dir}/bin/pip3" install "${pip_args[@]}" -r "$requirements"
}

resolve_patchelf() {
  local version
  if [[ "$patchelf_exe" == */* ]]; then
    [[ -x "$patchelf_exe" ]] || {
      echo "patchelf executable not found: $patchelf_exe" >&2
      exit 1
    }
  else
    patchelf_exe=$(command -v "$patchelf_exe" 2>/dev/null || true)
    [[ -n "$patchelf_exe" ]] || {
      echo "patchelf 0.14.0 or newer is required to build the ARM64 Moirai venv" >&2
      exit 1
    }
  fi
  version=$("$patchelf_exe" --version | awk '{print $NF}')
  if [[ -z "$version" || "$(printf '%s\n%s\n' '0.14.0' "$version" | sort -V | head -n1)" != "0.14.0" ]]; then
    echo "patchelf 0.14.0 or newer is required; found ${version:-unknown}: $patchelf_exe" >&2
    exit 1
  fi
}

patch_moirai_jaxlib_for_arm64() {
  local venv_dir="$1"
  local libjax_common

  [[ "$arch" == "arm64" ]] || return 0
  resolve_patchelf

  libjax_common=$(find "$venv_dir" -path '*/site-packages/jaxlib/libjax_common.so' -type f -print -quit)
  [[ -n "$libjax_common" ]] || {
    echo "Unable to locate jaxlib/libjax_common.so in $venv_dir" >&2
    exit 1
  }

  # Kylin V10's librt does not export clock_gettime@GLIBC_2.17, while libc
  # does. The ARM64 jaxlib wheel incorrectly records librt as the provider.
  if "$patchelf_exe" --print-needed "$libjax_common" | grep -qx 'librt.so.1'; then
    "$patchelf_exe" --replace-needed librt.so.1 libc.so.6 "$libjax_common"
  fi
  if "$patchelf_exe" --print-needed "$libjax_common" | grep -qx 'librt.so.1'; then
    echo "Failed to replace librt.so.1 dependency in $libjax_common" >&2
    exit 1
  fi
}

requirements_dir=$(mktemp -d "${work_dir}/requirements.XXXXXX")
cleanup_paths+=("$requirements_dir")
prepare_requirements "$requirements_dir"

archive_prefix="tdengine-tdgpt"
main_requirements="${requirements_dir}/requirements_ess.txt"
if [[ "$lite" == "1" ]]; then
  archive_prefix="tdengine-tdgpt-lite"
  main_requirements="${requirements_dir}/requirements_docker.txt"
fi

archives=()
if [[ "$bundle" == "all" || "$bundle" == "main" ]]; then
  main_stage=$(mktemp -d "${work_dir}/main.XXXXXX")
  cleanup_paths+=("$main_stage")
  main_venv="${main_stage}/venv"
  create_venv "$main_requirements" "$main_venv"
  "${main_venv}/bin/python3" -c 'import flask; print("tdgpt-venv-ok")'
  rewrite_runtime_refs "$main_venv"
  main_archive="${output_dir}/${archive_prefix}-venv-${venv_version}-linux-${arch}.tar.gz"
  rm -f "$main_archive"
  tar -C "$main_venv" -czf "$main_archive" .
  archives+=("$main_archive")
fi

if [[ "$bundle" == "all" || "$bundle" == "model" ]]; then
  model_stage=$(mktemp -d "${work_dir}/model.XXXXXX")
  cleanup_paths+=("$model_stage")
  moirai_venv="${model_stage}/venvs/moirai_venv"
  moment_venv="${model_stage}/venvs/momentfm_venv"
  mkdir -p "${model_stage}/venvs"
  create_venv "${requirements_dir}/requirements_moirai.txt" "$moirai_venv"
  create_venv "${requirements_dir}/requirements_moment.txt" "$moment_venv"
  PATH="${moment_venv}/bin:${PATH}" "${moment_venv}/bin/pip3" install --no-deps momentfm==0.1.4
  patch_moirai_jaxlib_for_arm64 "$moirai_venv"
  "${moirai_venv}/bin/python3" -c 'from uni2ts.model.moirai_moe import MoiraiMoEForecast; print("moirai-venv-ok")'
  "${moirai_venv}/bin/python3" -c 'import jax, jaxlib; print("moirai-jax-ok", jax.__version__, jaxlib.__version__)'
  "${moment_venv}/bin/python3" -c 'from momentfm import MOMENTPipeline; print("moment-venv-ok")'
  rewrite_runtime_refs "$moirai_venv"
  rewrite_runtime_refs "$moment_venv"
  model_archive="${output_dir}/${archive_prefix}-model-venv-${venv_version}-linux-${arch}.tar.gz"
  rm -f "$model_archive"
  tar -C "$model_stage" -czf "$model_archive" venvs
  archives+=("$model_archive")
fi

if [[ "$bundle" == "all" ]]; then
  tdmodel_stage=$(mktemp -d "${work_dir}/tdmodel.XXXXXX")
  cleanup_paths+=("$tdmodel_stage")
  tdmodel_venv="${tdmodel_stage}/tdmodel-venv"
  create_venv "$tdmodel_requirements" "$tdmodel_venv"
  "${tdmodel_venv}/bin/python3" -c 'import flask; print("tdmodel-venv-ok")'
  rewrite_runtime_refs "$tdmodel_venv"
  tdmodel_archive="${output_dir}/tdengine-tdmodel-venv-${venv_version}-linux-${arch}.tar.gz"
  rm -f "$tdmodel_archive"
  tar -C "$tdmodel_stage" -czf "$tdmodel_archive" tdmodel-venv
  archives+=("$tdmodel_archive")
fi

sha256sum "${archives[@]}"
