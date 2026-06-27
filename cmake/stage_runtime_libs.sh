#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  stage_runtime_libs.sh copy-glob <src-dir> <dst-dir> <glob>...
  stage_runtime_libs.sh copy-ldd  <src-lib> <dst-dir>
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
      cp -Lf "${src}" "${dst_dir}/$(basename "${src}")"
    done
  done
}

copy_ldd() {
  local src_lib=$1
  local dst_dir=$2

  mkdir -p "${dst_dir}"
  while IFS= read -r dep; do
    [ -n "${dep}" ] || continue
    cp -Lf "${dep}" "${dst_dir}/$(basename "${dep}")"
  done < <(ldd "${src_lib}" | awk '/libssl\.so|libcrypto\.so/ {print $3}' | sort -u)
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
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
