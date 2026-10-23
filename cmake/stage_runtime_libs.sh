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

usage() {
  cat <<'EOF'
Usage:
  stage_runtime_libs.sh copy-glob <src-dir> <dst-dir> <glob>...
  stage_runtime_libs.sh copy-ldd  <src-lib> <dst-dir>
  stage_runtime_libs.sh copy-ldd-glob <src-dir> <dst-dir> <glob>...
  stage_runtime_libs.sh fix-darwin-rpaths <dst-dir>
  stage_runtime_libs.sh fix-linux-rpaths <dst-dir>
  stage_runtime_libs.sh copy-msodbc-resources <src-dir> <dst-dir>
EOF
}

copy_glob() {
  local src_dir=$1
  local dst_dir=$2
  shift 2

  mkdir -p "${dst_dir}"
  shopt -s nullglob
  for pattern in "$@"; do
    for src in "${src_dir}"/${pattern}; do
      [ -e "${src}" ] || continue
      copy_to_dir "${src}" "${dst_dir}"
    done
  done
}

copy_to_dir() {
  local src=$1
  local dst_dir=$2
  local dst="${dst_dir}/$(basename "${src}")"

  [ -e "${dst}" ] && [ "${src}" -ef "${dst}" ] && return 0
  cp -Lf "${src}" "${dst}"
}

prune_toolchain_runtime_libs() {
  local dst_dir=$1
  local lib

  shopt -s nullglob
  for lib in "${dst_dir}"/libstdc++.so* \
             "${dst_dir}"/libgcc_s.so* \
             "${dst_dir}"/libasan.so* \
             "${dst_dir}"/libubsan.so* \
             "${dst_dir}"/libtsan.so* \
             "${dst_dir}"/liblsan.so*; do
    rm -f "${lib}"
  done
}

resolve_darwin_dep() {
  local dep=$1
  local owner=$2
  local owner_dir
  local rpath
  local candidate

  case "${dep}" in
    /*)
      printf '%s\n' "${dep}"
      return 0
      ;;
  esac

  owner_dir="$(dirname "${owner}")"
  case "${dep}" in
    @loader_path/*)
      candidate="${owner_dir}/${dep#@loader_path/}"
      ;;
    @rpath/*)
      while IFS= read -r rpath; do
        case "${rpath}" in
          @loader_path)
            rpath="${owner_dir}"
            ;;
          @loader_path/*)
            rpath="${owner_dir}/${rpath#@loader_path/}"
            ;;
          /*)
            ;;
          *)
            continue
            ;;
        esac

        candidate="${rpath}/${dep#@rpath/}"
        if [ -e "${candidate}" ]; then
          printf '%s\n' "${candidate}"
          return 0
        fi
      done < <(otool -l "${owner}" | awk '
        $1 == "cmd" && $2 == "LC_RPATH" { in_rpath = 1; next }
        in_rpath && $1 == "path" { print $2; in_rpath = 0 }
      ')

      candidate="${owner_dir}/${dep#@rpath/}"
      if [ -e "${candidate}" ]; then
        printf '%s\n' "${candidate}"
        return 0
      fi
      return 1
      ;;
    *)
      return 1
      ;;
  esac

  [ -e "${candidate}" ] || return 1
  printf '%s\n' "${candidate}"
}

copy_ldd() {
  local src_lib=$1
  local dst_dir=$2
  local dep=""
  local raw_dep=""
  local cur=""
  local seen=""
  local queue=("${src_lib}")

  mkdir -p "${dst_dir}"
  if command -v otool >/dev/null 2>&1 && otool -L "${src_lib}" >/dev/null 2>&1; then
    while [ ${#queue[@]} -gt 0 ]; do
      cur="${queue[0]}"
      queue=("${queue[@]:1}")
      otool -L "${cur}" >/dev/null 2>&1 || continue
      while IFS= read -r raw_dep; do
        [ -n "${raw_dep}" ] || continue
        dep="$(resolve_darwin_dep "${raw_dep}" "${cur}")" || continue
        [ "$(basename "${dep}")" = "$(basename "${cur}")" ] && continue
        should_skip_ldd_dep "${dep}" && continue
        [ -e "${dep}" ] || continue
        case "${seen}" in
          *"|${dep}|"*) continue ;;
        esac
        seen="${seen}|${dep}|"
        copy_to_dir "${dep}" "${dst_dir}"
        queue+=("${dep}")
      done < <(otool -L "${cur}" | awk 'NR > 1 {print $1}' | sort -u)
    done
    prune_toolchain_runtime_libs "${dst_dir}"
    return 0
  fi

  command -v ldd >/dev/null 2>&1 || return 0
  while [ ${#queue[@]} -gt 0 ]; do
    cur="${queue[0]}"
    queue=("${queue[@]:1}")
    ldd "${cur}" >/dev/null 2>&1 || continue
    while IFS= read -r dep; do
      [ -n "${dep}" ] || continue
      should_skip_ldd_dep "${dep}" && continue
      [ -e "${dep}" ] || continue
      case "${seen}" in
        *"|${dep}|"*) continue ;;
      esac
      seen="${seen}|${dep}|"
      copy_to_dir "${dep}" "${dst_dir}"
      queue+=("${dep}")
    done < <(ldd "${cur}" | awk '/=> \// {print $3} /^[[:space:]]*\/.*\.so/ {print $1}' | sort -u)
  done
  prune_toolchain_runtime_libs "${dst_dir}"
}

fix_darwin_rpaths() {
  local dst_dir=$1
  local lib=""
  local dep=""
  local base=""

  command -v otool >/dev/null 2>&1 || return 0
  command -v install_name_tool >/dev/null 2>&1 || return 0

  shopt -s nullglob
  for lib in "${dst_dir}"/lib*.dylib "${dst_dir}"/lib*.dylib.*; do
    [ -f "${lib}" ] || continue
    base="$(basename "${lib}")"
    install_name_tool -id "@loader_path/${base}" "${lib}" 2>/dev/null || true
    while IFS= read -r dep; do
      [ -n "${dep}" ] || continue
      case "${dep}" in
        @loader_path/*) continue ;;
      esac
      should_skip_ldd_dep "${dep}" && continue
      base="$(basename "${dep}")"
      [ -e "${dst_dir}/${base}" ] || continue
      install_name_tool -change "${dep}" "@loader_path/${base}" "${lib}" 2>/dev/null || true
    done < <(otool -L "${lib}" | awk 'NR > 1 {print $1}' | sort -u)
  done
}

copy_msodbc_resources() {
  local src_dir=$1 dst_dir=$2
  local resources="${src_dir}/../share/resources"
  [ -f "${resources}/en_US/msodbcsqlr18.rll" ] || {
    echo "Microsoft ODBC resources missing next to ${src_dir}" >&2
    return 1
  }
  mkdir -p "${dst_dir}/../share/resources"
  cp -a "${resources}/." "${dst_dir}/../share/resources/"
}

# Every bundled ELF needs its own RUNPATH: DT_RUNPATH is not transitive.
# Patch copies only; the external dependency cache must remain untouched.
fix_linux_rpaths() {
  local dst_dir=$1 lib dynamic
  [ "$(uname -s)" = Linux ] || return 0
  command -v readelf >/dev/null 2>&1 || {
    echo "readelf is required to inspect private FQ runtime libraries" >&2
    return 1
  }
  shopt -s nullglob
  for lib in "${dst_dir}"/lib*.so*; do
    [ -f "${lib}" ] && [ ! -L "${lib}" ] || continue
    case "$(basename "${lib}")" in
      libtaos.so*|libtaosnative.so*|libtaosws.so*|libtaospyudf.so*) continue ;;
    esac
    # Replace build-machine paths. The same layout is used by build/lib and
    # installed driver/. Microsoft driver resources stay in ../share/resources.
    dynamic=$(readelf -d "${lib}" 2>&1) || {
      echo "Cannot inspect FQ runtime ${lib}: ${dynamic}" >&2
      return 1
    }
    if printf '%s\n' "${dynamic}" | grep -Eq '\(RUNPATH\).*\[\$ORIGIN\]'; then
      continue
    fi
    command -v patchelf >/dev/null 2>&1 || {
      echo "patchelf is required to prepare private FQ runtime library: ${lib}" >&2
      return 1
    }
    patchelf --set-rpath '$ORIGIN' "${lib}"
  done
}

should_skip_ldd_dep() {
  local dep=$1
  local base
  case "${dep}" in
    /System/Library/*|/usr/lib/*)
      return 0
      ;;
  esac
  base="$(basename "${dep}")"
  case "${base}" in
    libstdc++.so*|libgcc_s.so*|libasan.so*|libubsan.so*|libtsan.so*|liblsan.so*)
      return 0
      ;;
  esac
  case "${base}" in
    ld-linux*.so*|linux-vdso*.so*|libBrokenLocale.so*|libSystem*.dylib|libanl.so*|libc.so*|libdl.so*|libm.so*|libmvec.so*|libpthread.so*|libresolv.so*|librt.so*|libthread_db.so*|libutil.so*)
      return 0
      ;;
  esac
  # libselinux ships with the base OS and is linked by coreutils (find, mkdir,
  # cp, ...).  On some build images the staged closure pulls it in (e.g. an
  # RHEL-style libkrb5support needed by libmsodbcsql links it), and the copied
  # build-machine build has no version symbols.  Shadowing the container's own
  # copy through LD_LIBRARY_PATH then makes every one of those tools print
  # "no version information available" on stderr — which breaks tests that
  # parse merged tool output (12-UDFs locates UDF libraries with `find`).
  # The host copy is always the right one, so never stage it.
  case "${base}" in
    libselinux.so*)
      return 0
      ;;
  esac
  return 1
}

copy_ldd_glob() {
  local src_dir=$1
  local dst_dir=$2
  local pattern=""
  local src=""
  shift 2

  shopt -s nullglob
  for pattern in "$@"; do
    for src in "${src_dir}"/${pattern}; do
      copy_ldd "${src}" "${dst_dir}"
    done
  done
  prune_toolchain_runtime_libs "${dst_dir}"
}

main() {
  local mode=${1:-}
  case "${mode}" in
    copy-glob)
      [ $# -ge 4 ] || { usage >&2; exit 1; }
      shift
      copy_glob "$@"
      ;;
    copy-ldd)
      [ $# -eq 3 ] || { usage >&2; exit 1; }
      shift
      copy_ldd "$@"
      ;;
    copy-ldd-glob)
      [ $# -ge 4 ] || { usage >&2; exit 1; }
      shift
      copy_ldd_glob "$@"
      ;;
    copy-msodbc-resources)
      [ $# -eq 3 ] || { usage >&2; exit 1; }
      shift
      copy_msodbc_resources "$@"
      ;;
    fix-linux-rpaths)
      [ $# -eq 2 ] || { usage >&2; exit 1; }
      shift
      fix_linux_rpaths "$@"
      ;;
    fix-darwin-rpaths)
      [ $# -eq 2 ] || { usage >&2; exit 1; }
      shift
      fix_darwin_rpaths "$@"
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
