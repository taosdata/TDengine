#!/bin/bash
# uninstall_adapter.sh — uninstall taosAdapter from the system

set -e

adapterBinary="taosadapter"
configFile="taosadapter.toml"
serviceFile="taosadapter.service"
installBin="/usr/bin/${adapterBinary}"
installConfig="/etc/taos/${configFile}"
installConfigNew="/etc/taos/${configFile}.new"
installService="/etc/systemd/system/${serviceFile}"

if [ "$(uname)" != "Linux" ]; then
  echo "Error: this uninstaller only supports Linux."
  exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Error: please run as root."
  exit 1
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl stop taosadapter >/dev/null 2>&1 || :
  systemctl disable taosadapter >/dev/null 2>&1 || :
fi

rm -f "$installBin" "$installConfig" "$installConfigNew" "$installService"

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload >/dev/null 2>&1 || :
fi

echo "taosAdapter uninstalled."
