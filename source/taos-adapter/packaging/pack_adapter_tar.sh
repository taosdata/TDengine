#!/bin/bash
#
# pack_adapter_tar.sh — Generate taosAdapter tar.gz package
#
# Usage:
#   ./pack_adapter_tar.sh -c <compile_dir> -n <version> [-m <compat_version>] [-V <verType>]
#
# Parameters:
#   -c  Compile directory (Go build output root containing bin/taosadapter)
#   -n  Version number (e.g. 3.3.6.0)
#   -m  Compatible version number (default: 3.0.0.0)
#   -V  Version type: stable | beta (default: stable)
#
# Output:
#   <adapter_dir>/release/taosAdapter-<ver>-Linux-<arch>.tar.gz

set -e

version=""
versionComp="3.0.0.0"
verType="stable"
compile_dir=""

productName="taosAdapter"
adapterBinary="taosadapter"
configFile="taosadapter.toml"
serviceFile="taosadapter.service"

show_help() {
  echo "Usage: $(basename "$0") -c <compile_dir> -n <version> [-m <compat_version>] [-V stable|beta]"
  echo ""
  echo "  -c  Compile directory (Go build output root containing bin/taosadapter)"
  echo "  -n  Version number (e.g. 3.3.6.0)"
  echo "  -m  Compatible version number (default: 3.0.0.0)"
  echo "  -V  Version type: stable | beta (default: stable)"
}

while getopts "hc:n:m:V:" arg; do
  case $arg in
    c) compile_dir="$OPTARG" ;;
    n) version="$OPTARG" ;;
    m) versionComp="$OPTARG" ;;
    V) verType="$OPTARG" ;;
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

if [ -z "$compile_dir" ]; then
  echo "Error: compile directory (-c) is required"
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

script_dir="$(dirname "$(readlink -f "$0")")"
adapter_dir="$(readlink -f "${script_dir}/..")"

if [[ "$compile_dir" != /* ]]; then
  compile_dir="$(readlink -f "${adapter_dir}/${compile_dir}")"
fi

release_dir="${adapter_dir}/release"
config_src="${adapter_dir}/example/config/${configFile}"
service_src="${adapter_dir}/${serviceFile}"
install_src="${script_dir}/install_adapter.sh"
uninstall_src="${script_dir}/uninstall_adapter.sh"
binary_src="${compile_dir}/bin/${adapterBinary}"

if [ ! -d "$compile_dir" ]; then
  echo "Error: compile directory not found: $compile_dir"
  exit 1
fi

os_type=$(uname)
if [ "$os_type" != "Linux" ]; then
  echo "Error: this script only supports Linux. Detected: $os_type"
  exit 1
fi

arch_raw=$(uname -m)
case "$arch_raw" in
  x86_64)       arch="x64" ;;
  aarch64)      arch="arm64" ;;
  arm64)        arch="arm64" ;;
  mips64*)      arch="mips64" ;;
  loongarch64)  arch="loongarch64" ;;
  riscv64)      arch="riscv64" ;;
  *)
    echo "Warning: unrecognized architecture '$arch_raw', using as-is"
    arch="$arch_raw"
    ;;
esac

echo "============================================================"
echo "  taosAdapter tar.gz Packaging"
echo "  Version:    ${version}"
echo "  Compatible: ${versionComp}"
echo "  VerType:    ${verType}"
echo "  Arch:       ${arch} (${arch_raw})"
echo "  Build dir:  ${compile_dir}"
echo "  Output dir: ${release_dir}"
echo "============================================================"

for f in "$binary_src" "$config_src" "$service_src" "$install_src" "$uninstall_src"; do
  if [ ! -f "$f" ]; then
    echo "Error: required file not found: $f"
    exit 1
  fi
done

pkg_name="${productName}-${version}"
pkg_dir="${release_dir}/${pkg_name}"

rm -rf "$pkg_dir"
mkdir -p "${pkg_dir}/bin" "${pkg_dir}/cfg"

cp "$binary_src" "${pkg_dir}/bin/${adapterBinary}"
cp "$config_src" "${pkg_dir}/cfg/${configFile}"
cp "$service_src" "${pkg_dir}/cfg/${serviceFile}"
chmod a+x "${pkg_dir}/bin/${adapterBinary}"

echo "  Creating inner package.tar.gz..."
cd "$pkg_dir"
tar -zcf package.tar.gz bin cfg --remove-files

cp "$install_src" "${pkg_dir}/install_adapter.sh"
cp "$uninstall_src" "${pkg_dir}/uninstall_adapter.sh"
chmod a+x "${pkg_dir}/install_adapter.sh" "${pkg_dir}/uninstall_adapter.sh"

echo "  Creating outer tar.gz..."
mkdir -p "$release_dir"
cd "$release_dir"
if [[ "$verType" == "beta" ]]; then
  tar_name="${pkg_name}-beta-${os_type}-${arch}.tar.gz"
else
  tar_name="${pkg_name}-${os_type}-${arch}.tar.gz"
fi

tar -zcf "$tar_name" "$(basename "$pkg_dir")" --remove-files

echo ""
echo "============================================================"
echo "  Packaging complete!"
echo "  Package: ${release_dir}/${tar_name}"
echo "============================================================"
