#!/usr/bin/env bash

# Shared provider-runtime setup for pytest-based federated-query tests.
# Source this before pytest starts taosd.

_fq_runtime_prepend_path() {
  local name="$1"
  local dir="$2"
  local cur="${!name:-}"

  case ":${cur}:" in
    *":${dir}:"*) ;;
    *) export "${name}=${dir}${cur:+:${cur}}" ;;
  esac
}

_fq_runtime_should_enable() {
  local arg

  case "${FQ_RUNTIME_LIBS:-auto}" in
    1|on|ON|true|TRUE) return 0 ;;
    0|off|OFF|false|FALSE) return 1 ;;
  esac

  for arg in "$@"; do
    case "${arg}" in
      *09-DataQuerying/19-FederatedQuery/*|*19-FederatedQuery/*)
        return 0
        ;;
      *05-VirtualTables/*ExtSource*|*05-VirtualTables/06-Meta/test_vtable_series*)
        return 0
        ;;
    esac
  done

  return 1
}

fq_runtime_libs_setup() {
  local build_dir="${1:-${BUILD_DIR:-}}"
  local runtime_dir
  shift || true

  _fq_runtime_should_enable "$@" || return 0

  if [ -n "${build_dir}" ]; then
    runtime_dir="${build_dir%/}/build/lib"
  else
    runtime_dir=""
  fi
  if [ -z "${runtime_dir}" ] || [ ! -d "${runtime_dir}" ]; then
    echo "[fq-runtime] WARN: normal runtime library dir not found: ${runtime_dir:-<unset>}" >&2
    return 0
  fi
  _fq_runtime_prepend_path LD_LIBRARY_PATH "${runtime_dir}"
  if [ "$(uname -s 2>/dev/null)" = "Darwin" ]; then
    _fq_runtime_prepend_path DYLD_LIBRARY_PATH "${runtime_dir}"
  fi
  echo "[fq-runtime] using provider runtime libraries: ${runtime_dir}"
}
