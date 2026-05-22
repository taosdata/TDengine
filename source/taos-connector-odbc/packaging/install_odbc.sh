#!/bin/bash

set -e

silent_mode=0
tar_name="package.tar.gz"
os_type=$(uname)
script_dir="$(dirname "$(readlink -f "$0")")"
staging_dir="${script_dir}/.install-odbc-package"
template_dir="/etc/taos/odbc"

show_help() {
  cat <<EOF
Usage: $(basename "$0") [-s]

Options:
  -s  Silent install
  -h  Show help
EOF
}

log() {
  if [ "$silent_mode" -eq 0 ]; then
    echo "$@"
  fi
}

warn() {
  echo "Warning: $@" >&2
}

cleanup() {
  rm -rf "$staging_dir"
}
trap cleanup EXIT

resolve_lib_dir() {
  if [ -d "/usr/local/lib" ]; then
    echo "/usr/local/lib"
    return
  fi

  if [ "$(getconf LONG_BIT 2>/dev/null || echo 64)" = "64" ] && [ -d "/usr/lib64" ]; then
    echo "/usr/lib64"
    return
  fi

  echo "/usr/local/lib"
}

resolve_extra_lib_dir() {
  if [ "$(getconf LONG_BIT 2>/dev/null || echo 64)" = "64" ] && [ -d "/usr/lib64" ] && [ "/usr/lib64" != "$1" ]; then
    echo "/usr/lib64"
  fi
}

have_libtaos() {
  if command -v ldconfig >/dev/null 2>&1 && ldconfig -p 2>/dev/null | grep -q 'libtaos\.so'; then
    return 0
  fi

  for candidate in \
    /usr/local/lib/libtaos.so \
    /usr/local/lib64/libtaos.so \
    /usr/lib/libtaos.so \
    /usr/lib64/libtaos.so \
    /lib/libtaos.so \
    /lib64/libtaos.so; do
    [ -e "$candidate" ] && return 0
  done

  return 1
}

while getopts "sh" arg; do
  case $arg in
    s) silent_mode=1 ;;
    h)
      show_help
      exit 0
      ;;
    ?)
      echo "Unknown argument. Use -h for help."
      exit 1
      ;;
  esac
done

if [ "$os_type" != "Linux" ]; then
  echo "Error: this installer only supports Linux. Detected: $os_type"
  exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Error: install_odbc.sh must be run as root"
  exit 1
fi

if [ ! -f "${script_dir}/${tar_name}" ]; then
  echo "Error: ${tar_name} not found in ${script_dir}"
  exit 1
fi

lib_dir="$(resolve_lib_dir)"
extra_lib_dir="$(resolve_extra_lib_dir "$lib_dir")"
mkdir -p "$staging_dir" "$lib_dir" "$template_dir"
[ -n "$extra_lib_dir" ] && mkdir -p "$extra_lib_dir"

tar -zxf "${script_dir}/${tar_name}" -C "$staging_dir"

if [ ! -d "${staging_dir}/lib" ]; then
  echo "Error: package content is missing lib/"
  exit 1
fi

if [ ! -d "${staging_dir}/templates" ]; then
  echo "Error: package content is missing templates/"
  exit 1
fi

log "Installing libtaos_odbc.so* to ${lib_dir} ..."
find "$lib_dir" -maxdepth 1 -name 'libtaos_odbc.so*' -exec rm -f {} \; 2>/dev/null || :
cp -a "${staging_dir}/lib/." "$lib_dir/"

if [ -n "$extra_lib_dir" ]; then
  log "Mirroring libtaos_odbc.so* to ${extra_lib_dir} ..."
  find "$extra_lib_dir" -maxdepth 1 -name 'libtaos_odbc.so*' -exec rm -f {} \; 2>/dev/null || :
  cp -a "${staging_dir}/lib/." "$extra_lib_dir/"
fi

log "Installing ODBC templates to ${template_dir} ..."
cp -f "${staging_dir}/templates/odbc.ini.in" "$template_dir/"
cp -f "${staging_dir}/templates/odbcinst.ini.in" "$template_dir/"

if command -v ldconfig >/dev/null 2>&1; then
  log "Refreshing linker cache ..."
  ldconfig
else
  warn "ldconfig command not found; refresh the dynamic linker cache manually if needed."
fi

if ! have_libtaos; then
  warn "libtaos.so was not found in the linker cache or common library paths. Install taos-community/TDengine client libraries before using the ODBC driver."
fi

if ! command -v odbcinst >/dev/null 2>&1; then
  warn "odbcinst command not found. Install unixODBC (and odbcinst) before registering the driver or DSN."
fi

cat <<EOF
TDengine ODBC connector installed.

Library path: ${lib_dir}
Template path: ${template_dir}

Next steps:
  1. Review ${template_dir}/odbcinst.ini.in and register the driver with unixODBC.
  2. Copy ${template_dir}/odbc.ini.in to /etc/odbc.ini or ~/.odbc.ini and fill in your TDengine connection settings.
  3. Run 'odbcinst -j' to inspect the active unixODBC configuration paths.
EOF
