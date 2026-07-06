#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  stage_runtime_libs.sh copy-glob <src-dir> <dst-dir> <glob>...
  stage_runtime_libs.sh copy-ldd  <src-lib> <dst-dir>
  stage_runtime_libs.sh copy-ldd-glob <src-dir> <dst-dir> <glob>...
  stage_runtime_libs.sh fix-darwin-rpaths <dst-dir>
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

copy_ldd() {
  local src_lib=$1
  local dst_dir=$2
  local dep=""
  local cur=""
  local seen=""
  local queue=("${src_lib}")

  mkdir -p "${dst_dir}"
  if command -v otool >/dev/null 2>&1 && otool -L "${src_lib}" >/dev/null 2>&1; then
    while [ ${#queue[@]} -gt 0 ]; do
      cur="${queue[0]}"
      queue=("${queue[@]:1}")
      otool -L "${cur}" >/dev/null 2>&1 || continue
      while IFS= read -r dep; do
        [ -n "${dep}" ] || continue
        [ "$(basename "${dep}")" = "$(basename "${cur}")" ] && continue
        should_skip_ldd_dep "${dep}" && continue
        [ -e "${dep}" ] || continue
        case "${seen}" in
          *"|${dep}|"*) continue ;;
        esac
        seen="${seen}|${dep}|"
        copy_to_dir "${dep}" "${dst_dir}"
        queue+=("${dep}")
      done < <(otool -L "${cur}" | awk 'NR > 1 && $1 ~ /^\// {print $1}' | sort -u)
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
