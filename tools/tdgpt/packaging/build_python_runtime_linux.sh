#!/usr/bin/env bash

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

set -euo pipefail

work_dir="${PWD}/tdgpt-python-runtime-build"
output_dir="${PWD}/dist"
python_version="3.12.12"
arch=""
uv_exe=""
patchelf_bin=""

usage() {
  cat <<'EOF'
Usage: build_python_runtime_linux.sh [options]

Options:
  --python-version VERSION  CPython version. Default: 3.12.12
  --work-dir DIR            Build workspace. Default: ./tdgpt-python-runtime-build
  --output-dir DIR          Archive output directory. Default: ./dist
  --arch x64|arm64          Validate the native architecture and name the output.
  --uv-exe PATH             uv executable. Default: uv from PATH.
  --patchelf PATH           patchelf executable. Default: patchelf from PATH.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-version) python_version="$2"; shift 2 ;;
    --work-dir) work_dir="$2"; shift 2 ;;
    --output-dir) output_dir="$2"; shift 2 ;;
    --arch) arch="$2"; shift 2 ;;
    --uv-exe) uv_exe="$2"; shift 2 ;;
    --patchelf) patchelf_bin="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

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

uv_exe="${uv_exe:-$(command -v uv || true)}"
[[ -n "$uv_exe" && -x "$uv_exe" ]] || {
  echo "uv is required to build the portable Python runtime; install uv or pass --uv-exe PATH" >&2
  exit 1
}
patchelf_bin="${patchelf_bin:-$(command -v patchelf || true)}"
[[ -n "$patchelf_bin" && -x "$patchelf_bin" ]] || {
  echo "patchelf is required to make the Python runtime relocatable; install it or pass --patchelf PATH" >&2
  exit 1
}

uv_store="${work_dir}/uv-python-store"
runtime_dir="${work_dir}/python-runtime-${python_version}"
output_file="${output_dir}/tdengine-python-runtime-${python_version}-linux-${arch}.tar.gz"

runtime_elf_files() {
  find "${runtime_dir}/bin" "${runtime_dir}/lib" -type f \
    \( -perm -111 -o -name '*.so' -o -name '*.so.*' \) -print0
}

rpath_for_runtime_file() {
  case "$1" in
    "${runtime_dir}/bin/"*) printf '%s\n' '$ORIGIN/../lib' ;;
    "${runtime_dir}/lib/"*)
      if [[ "$(dirname "$1")" == "${runtime_dir}/lib" ]]; then
        printf '%s\n' '$ORIGIN'
      else
        printf '%s\n' '$ORIGIN/../..'
      fi
      ;;
  esac
}

patch_runtime_rpaths() {
  local file
  local rpath

  while IFS= read -r -d '' file; do
    readelf -d "$file" 2>/dev/null | grep -q 'Dynamic section' || continue
    rpath="$(rpath_for_runtime_file "$file")"
    "$patchelf_bin" --set-rpath "$rpath" "$file"
  done < <(runtime_elf_files)
}

verify_runtime_dependencies() {
  local file
  local failed=0

  while IFS= read -r -d '' file; do
    readelf -d "$file" 2>/dev/null | grep -q 'Dynamic section' || continue
    if env -u LD_LIBRARY_PATH ldd "$file" | grep -q 'not found'; then
      echo "Unresolved runtime dependency: $file" >&2
      env -u LD_LIBRARY_PATH ldd "$file" >&2 || true
      failed=1
    fi
  done < <(runtime_elf_files)

  [[ "$failed" -eq 0 ]] || exit 1
}

build_sqlite_extension_if_missing() {
  local python_include
  local python_source_archive
  local python_source_dir
  local extension_suffix
  local extension_dir
  local compiler

  if env -u LD_LIBRARY_PATH "${runtime_dir}/bin/python3" -c 'import sqlite3' 2>/dev/null; then
    return
  fi

  [[ -f /usr/include/sqlite3.h ]] || {
    echo "The managed Python runtime does not provide sqlite3." >&2
    echo "Install libsqlite3-dev and rebuild the runtime." >&2
    exit 1
  }

  compiler="${CC:-cc}"
  command -v "$compiler" >/dev/null 2>&1 || {
    echo "A C compiler is required to build the missing sqlite3 extension." >&2
    exit 1
  }

  python_source_archive="${work_dir}/Python-${python_version}.tgz"
  python_source_dir="${work_dir}/Python-${python_version}"
  if [[ ! -d "$python_source_dir" ]]; then
    if [[ ! -f "$python_source_archive" ]]; then
      if command -v curl >/dev/null 2>&1; then
        curl -fL --retry 3 \
          -o "$python_source_archive" \
          "https://www.python.org/ftp/python/${python_version}/Python-${python_version}.tgz"
      elif command -v wget >/dev/null 2>&1; then
        wget -O "$python_source_archive" \
          "https://www.python.org/ftp/python/${python_version}/Python-${python_version}.tgz"
      else
        echo "curl or wget is required to download the CPython source archive." >&2
        exit 1
      fi
    fi
    tar -C "$work_dir" -xzf "$python_source_archive"
  fi

  python_include="$(env -u LD_LIBRARY_PATH "${runtime_dir}/bin/python3" -c \
    'import sysconfig; print(sysconfig.get_path("include"))')"
  extension_suffix="$(env -u LD_LIBRARY_PATH "${runtime_dir}/bin/python3" -c \
    'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))')"
  extension_dir="$(env -u LD_LIBRARY_PATH "${runtime_dir}/bin/python3" -c \
    'import sysconfig; print(sysconfig.get_config_var("DESTSHARED"))')"
  mkdir -p "$extension_dir"

  "$compiler" -shared -fPIC -pthread -O3 \
    -I"$python_include" \
    -I"${python_source_dir}/Modules/_sqlite" \
    "${python_source_dir}"/Modules/_sqlite/*.c \
    -lsqlite3 \
    -o "${extension_dir}/_sqlite3${extension_suffix}"
}

mkdir -p "$work_dir" "$output_dir"
rm -rf "$uv_store" "$runtime_dir"
"$uv_exe" python install --managed-python --install-dir "$uv_store" "$python_version"

runtime_source="$(find "$uv_store" \( -type f -o -type l \) -path '*/bin/python3' -print -quit)"
[[ -n "$runtime_source" ]] || {
  echo "uv did not install a Python runtime under: $uv_store" >&2
  exit 1
}
runtime_source="$(dirname "$(dirname "$runtime_source")")"
case "$native_arch" in
  x64) [[ "$runtime_source" == *x86_64* ]] ;;
  arm64) [[ "$runtime_source" == *aarch64* || "$runtime_source" == *arm64* ]] ;;
esac || {
  echo "uv runtime architecture does not match requested ${native_arch}: $runtime_source" >&2
  exit 1
}
mkdir -p "$runtime_dir"
cp -a "${runtime_source}/." "$runtime_dir/"

"${runtime_dir}/bin/python3" --version | grep -Fx "Python ${python_version}"
build_sqlite_extension_if_missing
patch_runtime_rpaths
verify_runtime_dependencies
env -u LD_LIBRARY_PATH "${runtime_dir}/bin/python3" -c \
  'import _ctypes, _ssl, ctypes, curses, readline, sqlite3, ssl, tkinter'

rm -f "$output_file"
# Apex extracts this archive into <deps-root>/python-runtime, so bin/ is root.
tar -C "$runtime_dir" -czf "$output_file" .
sha256sum "$output_file"
