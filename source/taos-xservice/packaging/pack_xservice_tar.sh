#!/bin/bash
#
# pack_xservice_tar.sh — Generate taosX tar.gz packages (taosx + explorer or agent)
#
# Usage:
#   ./pack_xservice_tar.sh -c <deploy_dir> -n <version> [-m <compat_version>] [-V stable|beta] [-t taosx|agent]

set -e

version=""
versionComp="3.0.0.0"
verType="stable"
deploy_dir=""
package_type="taosx"
productName="taosX"

while getopts "hc:n:m:V:t:" arg; do
  case $arg in
    c) deploy_dir="$OPTARG" ;;
    n) version="$OPTARG" ;;
    m) versionComp="$OPTARG" ;;
    V) verType="$OPTARG" ;;
    t) package_type="$OPTARG" ;;
    h)
      echo "Usage: $(basename "$0") -c <deploy_dir> -n <version> [-m <compat_version>] [-V stable|beta] [-t taosx|agent]"
      echo ""
      echo "  -c  Pre-staged deploy directory"
      echo "  -n  Version number (e.g. 3.3.6.0)"
      echo "  -m  Compatible version number (default: 3.0.0.0)"
      echo "  -V  Version type: stable | beta (default: stable)"
      echo "  -t  Package type: taosx | agent (default: taosx)"
      exit 0
      ;;
    ?)
      echo "Unknown argument. Use -h for help."
      exit 1
      ;;
  esac
done

if [ -z "$deploy_dir" ]; then
  echo "Error: deploy directory (-c) is required"
  exit 1
fi

if [ -z "$version" ]; then
  echo "Error: version number (-n) is required"
  exit 1
fi

version_pattern='^([0-9]+\.){2,4}[0-9]+$'
if [[ ! "$version" =~ $version_pattern ]]; then
  echo "Error: invalid version format '$version' (expected e.g. 3.3.6.0)"
  exit 1
fi

if [[ "$verType" != "stable" && "$verType" != "beta" ]]; then
  echo "Error: verType must be 'stable' or 'beta'"
  exit 1
fi

if [[ "$package_type" != "taosx" && "$package_type" != "agent" ]]; then
  echo "Error: package type must be 'taosx' or 'agent'"
  exit 1
fi

script_dir="$(dirname "$(readlink -f "$0")")"
xservice_dir="$(readlink -f "${script_dir}/..")"
release_dir="${xservice_dir}/release"

if [[ "$deploy_dir" != /* ]]; then
  deploy_dir="$(readlink -f "${xservice_dir}/${deploy_dir}")"
fi

if [ ! -d "$deploy_dir" ]; then
  echo "Error: deploy directory not found: $deploy_dir"
  exit 1
fi

os_type=$(uname)
if [ "$os_type" != "Linux" ]; then
  echo "Error: this script only supports Linux. Detected: $os_type"
  exit 1
fi

arch_raw=$(uname -m)
case "$arch_raw" in
  x86_64)      arch="x64" ;;
  aarch64)     arch="arm64" ;;
  arm64)       arch="arm64" ;;
  mips64*)     arch="mips64" ;;
  loongarch64) arch="loongarch64" ;;
  riscv64)     arch="riscv64" ;;
  *)
    echo "Warning: unrecognized architecture '$arch_raw', using as-is"
    arch="$arch_raw"
    ;;
esac

copy_if_exists() {
  local src="$1"
  local dst="$2"
  if [ -f "$src" ]; then
    mkdir -p "$(dirname "$dst")"
    cp "$src" "$dst"
  fi
}

prepare_installer() {
  local src="$1"
  local dst="$2"
  local target_value="$3"

  python3 - "$src" "$dst" "$target_value" <<'PY'
from pathlib import Path
import sys

src, dst, target_value = sys.argv[1:4]
text = Path(src).read_text()
text = text.replace('target=""', f'target="{target_value}"', 1)
text = text.replace('if command -v sudo >/dev/null; then\n  csudo="sudo "\nfi', 'if [ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1; then\n  csudo="sudo "\nfi', 1)
helper_block = '''prepare_package_contents() {
  if [ -f ./package.tar.gz ]; then
    rm -rf ./bin ./plugins ./etc
    tar -zxf ./package.tar.gz
  fi
}

copy_plugins_if_any() {
  if [ -d ./plugins ] && [ -n "$(find ./plugins -mindepth 1 -print -quit 2>/dev/null)" ]; then
    echo "install plugins to ${TAOSX_ROOT_DIR}/plugins..."
    ${csudo}cp -fr plugins/* ${TAOSX_ROOT_DIR}/plugins
  else
    echo "no plugins to install, skipping"
  fi
}

'''
# IMPORTANT: replace plugin-copy pattern BEFORE inserting helper_block,
# otherwise the pattern also matches inside copy_plugins_if_any definition,
# creating infinite recursion → segfault.
text = text.replace('    echo "install plugins to ${TAOSX_ROOT_DIR}/plugins..."\n    ${csudo}cp -fr plugins/* ${TAOSX_ROOT_DIR}/plugins', '    copy_plugins_if_any')
text = text.replace('install_taosx_only() {', helper_block + 'install_taosx_only() {', 1)
text = text.replace('${csudo}systemctl daemon-reload', '${csudo}systemctl daemon-reload || :')
text = text.replace('${csudo}systemctl enable ${xName}', '${csudo}systemctl enable ${xName} || :')
text = text.replace('${csudo}systemctl enable ${explorerName}', '${csudo}systemctl enable ${explorerName} || :')
text = text.replace('# main entry point', 'prepare_package_contents\n\n# main entry point', 1)
Path(dst).write_text(text)
PY

  chmod a+x "$dst"
}

prepare_uninstaller() {
  local src="$1"
  local dst="$2"

  python3 - "$src" "$dst" <<'PY'
from pathlib import Path
import sys

src, dst = sys.argv[1:3]
text = Path(src).read_text()
text = text.replace('if command -v sudo >/dev/null; then\n  csudo="sudo "\nfi', 'if [ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1; then\n  csudo="sudo "\nfi', 1)
Path(dst).write_text(text)
PY

  chmod a+x "$dst"
}

mkdir -p "$release_dir"

if [ "$package_type" = "taosx" ]; then
  main_binary="taosx"
  outer_dir_name="${productName}-${version}"
  package_prefix="${productName}-${version}"
  installer_target="taosx"
else
  main_binary="taosx-agent"
  outer_dir_name="${productName}-agent-${version}"
  package_prefix="${productName}-agent-${version}"
  installer_target="taosx-agent"
fi

main_binary_path="${deploy_dir}/bin/${main_binary}"
if [ ! -f "$main_binary_path" ]; then
  echo "Error: required binary not found: $main_binary_path"
  exit 1
fi

outer_dir="${release_dir}/${outer_dir_name}"
rm -rf "$outer_dir"
mkdir -p "$outer_dir"

echo "============================================================"
echo "  taosX tar.gz Packaging"
echo "  Version:    ${version}"
echo "  Compatible: ${versionComp}"
echo "  VerType:    ${verType}"
echo "  Target:     ${package_type}"
echo "  Arch:       ${arch} (${arch_raw})"
echo "  Deploy dir: ${deploy_dir}"
echo "  Output dir: ${release_dir}"
echo "============================================================"

echo ""
echo ">>> Building ${package_type} package..."

mkdir -p "${outer_dir}/bin" "${outer_dir}/plugins" "${outer_dir}/etc/taos" "${outer_dir}/etc/systemd/system"

copy_if_exists "${deploy_dir}/bin/${main_binary}" "${outer_dir}/bin/${main_binary}"

if [ "$package_type" = "taosx" ]; then
  copy_if_exists "${deploy_dir}/bin/taos-explorer" "${outer_dir}/bin/taos-explorer"
  copy_if_exists "${deploy_dir}/etc/taos/taosx.toml" "${outer_dir}/etc/taos/taosx.toml"
  copy_if_exists "${deploy_dir}/etc/taos/explorer.toml" "${outer_dir}/etc/taos/explorer.toml"
  copy_if_exists "${deploy_dir}/etc/systemd/system/taosx.service" "${outer_dir}/etc/systemd/system/taosx.service"
  copy_if_exists "${deploy_dir}/etc/systemd/system/taos-explorer.service" "${outer_dir}/etc/systemd/system/taos-explorer.service"
else
  copy_if_exists "${deploy_dir}/etc/taos/agent.toml" "${outer_dir}/etc/taos/agent.toml"
  copy_if_exists "${deploy_dir}/etc/systemd/system/taosx-agent.service" "${outer_dir}/etc/systemd/system/taosx-agent.service"
fi

copy_if_exists "${deploy_dir}/plugins/opc/taosx-opc" "${outer_dir}/plugins/opc/taosx-opc"
copy_if_exists "${deploy_dir}/plugins/influxdb/taosx-influxdb.jar" "${outer_dir}/plugins/influxdb/taosx-influxdb.jar"
copy_if_exists "${deploy_dir}/plugins/opentsdb/taosx-opentsdb.jar" "${outer_dir}/plugins/opentsdb/taosx-opentsdb.jar"

chmod a+x "${outer_dir}/bin/"* 2>/dev/null || :
chmod a+x "${outer_dir}/plugins/opc/taosx-opc" 2>/dev/null || :

echo "  Creating inner package.tar.gz..."
(
  cd "$outer_dir"
  tar -zcf package.tar.gz bin plugins etc --remove-files
)

prepare_installer "${script_dir}/install.sh" "${outer_dir}/install.sh" "$installer_target"
prepare_uninstaller "${script_dir}/uninstall.sh" "${outer_dir}/uninstall.sh"

echo "  Creating outer tar.gz..."
(
  cd "$release_dir"
  rm -f "${package_prefix}-Linux-${arch}.tar.gz" "${package_prefix}-beta-Linux-${arch}.tar.gz"
  if [ "$verType" = "beta" ]; then
    tar_name="${package_prefix}-beta-Linux-${arch}.tar.gz"
  else
    tar_name="${package_prefix}-Linux-${arch}.tar.gz"
  fi
  tar -zcf "$tar_name" "$(basename "$outer_dir")" --remove-files
  echo "  ✓ Package: ${release_dir}/${tar_name}"
)

echo ""
echo "============================================================"
echo "  Packaging complete!"
ls -lh "${release_dir}"/*.tar.gz
echo "============================================================"
