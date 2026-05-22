#!/bin/bash

set -e

os_type=$(uname)
template_dir="/etc/taos/odbc"

show_help() {
  cat <<EOF
Usage: $(basename "$0")

Removes libtaos_odbc.so* from common system library directories and deletes
ODBC template files from /etc/taos/odbc.
EOF
}

resolve_lib_dirs() {
  local dirs=("/usr/local/lib")

  if [ -d "/usr/lib64" ]; then
    dirs+=("/usr/lib64")
  fi

  if [ -d "/usr/lib" ]; then
    dirs+=("/usr/lib")
  fi

  printf '%s\n' "${dirs[@]}" | awk '!seen[$0]++'
}

while getopts "h" arg; do
  case $arg in
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
  echo "Error: this uninstaller only supports Linux. Detected: $os_type"
  exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Error: uninstall_odbc.sh must be run as root"
  exit 1
fi

while IFS= read -r lib_dir; do
  [ -d "$lib_dir" ] || continue
  find "$lib_dir" -maxdepth 1 -name 'libtaos_odbc.so*' -exec rm -f {} \; 2>/dev/null || :
done < <(resolve_lib_dirs)

rm -f "${template_dir}/odbc.ini.in" "${template_dir}/odbcinst.ini.in" || :
rmdir "$template_dir" 2>/dev/null || :
rmdir /etc/taos 2>/dev/null || :

if command -v ldconfig >/dev/null 2>&1; then
  ldconfig
fi

echo "TDengine ODBC connector uninstalled."
