#!/bin/bash
#
# pack_gen_tar.sh — Generate taos-gen tar.gz package
#
# Usage:
#   ./pack_gen_tar.sh -c <compile_dir> -n <version> [-m <compat_version>] [-V stable|beta]

set -e

version=""
versionComp="3.0.0.0"
verType="stable"
compile_dir=""

productName="taosGen"
binaryName="taosgen"

while getopts "hc:n:m:V:" arg; do
  case $arg in
    c) compile_dir="$OPTARG" ;;
    n) version="$OPTARG" ;;
    m) versionComp="$OPTARG" ;;
    V) verType="$OPTARG" ;;
    h)
      echo "Usage: $(basename "$0") -c <compile_dir> -n <version> [-m <compat_version>] [-V stable|beta]"
      echo ""
      echo "  -c  Compile directory containing bin/taosgen"
      echo "  -n  Version number (e.g. 3.3.6.0)"
      echo "  -m  Compatible version number (default: 3.0.0.0)"
      echo "  -V  Version type: stable | beta (default: stable)"
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
gen_dir="$(readlink -f "${script_dir}/..")"
release_dir="${gen_dir}/release"

if [[ "$compile_dir" != /* ]]; then
  compile_dir="$(readlink -f "${gen_dir}/${compile_dir}")"
fi

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

binary_path="${compile_dir}/bin/${binaryName}"
if [ ! -f "$binary_path" ]; then
  echo "Error: required binary not found: $binary_path"
  exit 1
fi

for f in "${script_dir}/install_gen.sh" "${script_dir}/uninstall_gen.sh"; do
  if [ ! -f "$f" ]; then
    echo "Error: required packaging file not found: $f"
    exit 1
  fi
done

mkdir -p "$release_dir"

pkg_name="${productName}-${version}"
pkg_dir="${release_dir}/${pkg_name}"
rm -rf "$pkg_dir"
mkdir -p "$pkg_dir/bin"

cp "$binary_path" "$pkg_dir/bin/"
chmod a+x "$pkg_dir/bin/${binaryName}"

echo "$versionComp" > "${pkg_dir}/vercomp.txt"

cd "$pkg_dir"
tar -zcf package.tar.gz bin --remove-files
rm -f vercomp.txt

cp "${script_dir}/install_gen.sh" "$pkg_dir/"
cp "${script_dir}/uninstall_gen.sh" "$pkg_dir/"
chmod a+x "$pkg_dir/install_gen.sh" "$pkg_dir/uninstall_gen.sh"

cd "$release_dir"
if [[ "$verType" == "beta" ]]; then
  tar_name="${productName}-${version}-${verType}-${os_type}-${arch}.tar.gz"
else
  tar_name="${productName}-${version}-${os_type}-${arch}.tar.gz"
fi

tar -zcf "$tar_name" "$(basename "$pkg_dir")" --remove-files

echo "============================================================"
echo "  taos-gen tar.gz Packaging"
echo "  Version:    ${version}"
echo "  Compatible: ${versionComp}"
echo "  VerType:    ${verType}"
echo "  Arch:       ${arch} (${arch_raw})"
echo "  Output:     ${release_dir}/${tar_name}"
echo "============================================================"
ls -lh "${release_dir}/${tar_name}"
