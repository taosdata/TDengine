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

command -v xz >/dev/null 2>&1 || { echo "xz is required to build tar.xz packages (install xz-utils)" >&2; exit 1; }

work_dir="${PWD}/tdgpt-python-runtime-build"
output_dir="${PWD}/dist"
python_version="3.12.16"
arch=""
uv_exe=""
patchelf_bin=""
min_uv_version="0.13.9"

usage() {
  cat <<'EOF'
Usage: build_python_runtime_linux.sh [options]

Options:
  --python-version VERSION  CPython version. Default: 3.12.16
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
  echo "uv is required to build the portable Python runtime; install uv >= ${min_uv_version} or pass --uv-exe PATH" >&2
  exit 1
}
if ! uv_output="$("$uv_exe" --version 2>&1)"; then
  echo "Unable to query uv version ($uv_exe): $uv_output" >&2
  exit 1
fi
if [[ "$uv_output" =~ ^uv\ ([0-9]+\.[0-9]+\.[0-9]+)($|\ ) ]]; then
  uv_version="${BASH_REMATCH[1]}"
else
  echo "Unable to parse uv version ($uv_exe): $uv_output" >&2
  exit 1
fi
[[ -n "$uv_version" && "$(printf '%s\n%s\n' "$min_uv_version" "$uv_version" | sort -V | head -n1)" == "$min_uv_version" ]] || {
  echo "uv >= ${min_uv_version} is required to build the portable Python runtime; found ${uv_version:-unknown}" >&2
  exit 1
}
patchelf_bin="${patchelf_bin:-$(command -v patchelf || true)}"
[[ -n "$patchelf_bin" && -x "$patchelf_bin" ]] || {
  echo "patchelf is required to make the Python runtime relocatable; install it or pass --patchelf PATH" >&2
  exit 1
}

uv_store="${work_dir}/uv-python-store"
runtime_dir="${work_dir}/python-runtime-${python_version}"
output_file="${output_dir}/tdengine-python-runtime-${python_version}-linux-${arch}.tar.xz"

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
patch_runtime_rpaths
verify_runtime_dependencies
env -u LD_LIBRARY_PATH "${runtime_dir}/bin/python3" -c \
  'import _ctypes, _ssl, ctypes, curses, readline, sqlite3, ssl, tkinter'

rm -f "$output_file"
# Apex extracts this archive into <deps-root>/python-runtime, so bin/ is root.
tar -C "$runtime_dir" -cJf "$output_file" .
sha256sum "$output_file"
