#!/bin/bash
#
# pack_community_tar.sh — Generate community edition tar.gz packages (server + client)
#
# Usage:
#   ./pack_community_tar.sh -c <compile_dir> -n <version> [-m <compat_version>] [-V <verType>]
#
# Parameters:
#   -c  Compile directory (cmake build output, e.g. debug/)
#   -n  Version number (e.g. 3.3.6.0)
#   -m  Compatible version number (default: 3.0.0.0)
#   -V  Version type: stable | beta (default: stable)
#
# Output:
#   <community_dir>/release/TDengine-server-<ver>-Linux-<arch>.tar.gz
#   <community_dir>/release/TDengine-client-<ver>-Linux-<arch>.tar.gz

set -e

# ======================== Default Values ========================

version=""
versionComp="3.0.0.0"
verType="stable"
compile_dir=""

productName="TDengine"
clientName="taos"
serverName="taosd"
configFile="taos.cfg"

# ======================== Parse Arguments ========================

while getopts "hc:n:m:V:" arg; do
  case $arg in
    c) compile_dir="$OPTARG" ;;
    n) version="$OPTARG" ;;
    m) versionComp="$OPTARG" ;;
    V) verType="$OPTARG" ;;
    h)
      echo "Usage: $(basename $0) -c <compile_dir> -n <version> [-m <compat_version>] [-V stable|beta]"
      echo ""
      echo "  -c  Compile directory (cmake build output, e.g. debug/)"
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

# ======================== Validate Inputs ========================

if [ -z "$compile_dir" ]; then
  echo "Error: compile directory (-c) is required"
  exit 1
fi

if [ -z "$version" ]; then
  echo "Error: version number (-n) is required"
  exit 1
fi

# Validate version format: digits and dots only, 3-5 segments
version_pattern='^([0-9]+\.){2,4}[0-9]+$'
if [[ ! "$version" =~ $version_pattern ]]; then
  echo "Error: invalid version format '$version' (expected e.g. 3.3.6.0)"
  exit 1
fi

if [[ "$verType" != "stable" && "$verType" != "beta" && "$verType" != "preRelease" ]]; then
  echo "Error: verType must be 'stable', 'beta', or 'preRelease'"
  exit 1
fi

# ======================== Detect Environment ========================

script_dir="$(dirname "$(readlink -f "$0")")"
community_dir="$(readlink -f "${script_dir}/..")"

# Resolve compile_dir to absolute path
if [[ "$compile_dir" != /* ]]; then
  compile_dir="$(readlink -f "${community_dir}/${compile_dir}")"
fi

build_dir="${compile_dir}/build"
release_dir="${community_dir}/release"
code_dir="${community_dir}"
cfg_dir="${community_dir}/packaging/cfg"
tools_dir="${community_dir}/packaging/tools"

# Verify build directory exists
if [ ! -d "$build_dir" ]; then
  echo "Error: build directory not found: $build_dir"
  echo "Please run cmake and make before packaging."
  exit 1
fi

# Detect OS type
os_type=$(uname)
if [ "$os_type" != "Linux" ]; then
  echo "Error: this script only supports Linux. Detected: $os_type"
  exit 1
fi

# Detect CPU architecture
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
echo "  Community tar.gz Packaging"
echo "  Version:    ${version}"
echo "  Compatible: ${versionComp}"
echo "  VerType:    ${verType}"
echo "  Arch:       ${arch} (${arch_raw})"
echo "  Build dir:  ${build_dir}"
echo "  Output dir: ${release_dir}"
echo "============================================================"

# ======================== Verify Required Files ========================

# Required binaries
for bin in ${serverName} ${clientName}; do
  if [ ! -f "${build_dir}/bin/${bin}" ]; then
    echo "Error: required binary not found: ${build_dir}/bin/${bin}"
    exit 1
  fi
done

# Required libraries
if [ ! -f "${build_dir}/lib/libtaos.so" ]; then
  echo "Error: required library not found: ${build_dir}/lib/libtaos.so"
  exit 1
fi
if [ ! -f "${build_dir}/lib/libtaosnative.so" ]; then
  echo "Error: required library not found: ${build_dir}/lib/libtaosnative.so"
  exit 1
fi

# Required packaging files
for f in \
  "${tools_dir}/install.sh" \
  "${tools_dir}/install_client.sh" \
  "${tools_dir}/remove.sh" \
  "${tools_dir}/remove_client.sh" \
  "${cfg_dir}/${configFile}"; do
  if [ ! -f "$f" ]; then
    echo "Error: required packaging file not found: $f"
    exit 1
  fi
done

# ======================== Helper Functions ========================

# Copy a file if it exists; skip silently otherwise
copy_if_exists() {
  local src="$1"
  local dst="$2"
  if [ -f "$src" ]; then
    cp "$src" "$dst"
  fi
}

patch_server_install_messages() {
  local installer="$1"

  # The shared install.sh prints guidance for keeper/explorer/UI unconditionally.
  # Community tar packages do not ship those components, so trim the misleading
  # guidance from the copied installer.
  sed -i \
    -e '/To configure \${clientName}-explorer /d' \
    -e '/To start \${clientName}keeper /d' \
    -e '/To start \${clientName}-explorer /d' \
    -e '/Graphic User Interface/d' \
    "$installer"
}

# ======================== Build Server Package ========================

echo ""
echo ">>> Building server package..."

server_pkg_name="${productName}-server-${version}"
server_dir="${release_dir}/${server_pkg_name}"

# Clean previous output
rm -rf "${server_dir}"
mkdir -p "${server_dir}"

# --- Inner package: bin/ ---
mkdir -p "${server_dir}/bin"

# Core binaries (required)
cp "${build_dir}/bin/${serverName}" "${server_dir}/bin/"
cp "${build_dir}/bin/${clientName}" "${server_dir}/bin/"

# Optional binaries (may or may not be built)
copy_if_exists "${build_dir}/bin/taosBenchmark" "${server_dir}/bin/"
copy_if_exists "${build_dir}/bin/taosdump"      "${server_dir}/bin/"
copy_if_exists "${build_dir}/bin/taosudf"       "${server_dir}/bin/"

# Packaging helper scripts
copy_if_exists "${tools_dir}/remove.sh"         "${server_dir}/bin/"
copy_if_exists "${tools_dir}/set_core.sh"       "${server_dir}/bin/"
copy_if_exists "${tools_dir}/startPre.sh"       "${server_dir}/bin/"
copy_if_exists "${tools_dir}/taosd-dump-cfg.gdb" "${server_dir}/bin/"

chmod a+x "${server_dir}/bin/"* 2>/dev/null || :

# --- Inner package: cfg/ ---
mkdir -p "${server_dir}/cfg"
copy_if_exists "${cfg_dir}/${configFile}"     "${server_dir}/cfg/"
copy_if_exists "${cfg_dir}/${serverName}.service" "${server_dir}/cfg/"

# --- Inner package: inc/ ---
mkdir -p "${server_dir}/inc"
for header in \
  "${code_dir}/include/client/taos.h" \
  "${code_dir}/include/common/taosdef.h" \
  "${code_dir}/include/util/taoserror.h" \
  "${code_dir}/include/util/tdef.h" \
  "${code_dir}/include/libs/function/taosudf.h"; do
  copy_if_exists "$header" "${server_dir}/inc/"
done

# --- Create inner package.tar.gz ---
echo "  Creating inner package.tar.gz..."
cd "${server_dir}"
tar -zcf package.tar.gz bin cfg inc --remove-files

# --- Outer package: driver/ ---
mkdir -p "${server_dir}/driver"
cp "${build_dir}/lib/libtaos.so" "${server_dir}/driver/libtaos.so.${version}"
cp "${build_dir}/lib/libtaosnative.so" "${server_dir}/driver/libtaosnative.so.${version}"
echo "${versionComp}" > "${server_dir}/driver/vercomp.txt"

# --- Outer package: examples/ ---
if [ -d "${code_dir}/examples/c" ]; then
  mkdir -p "${server_dir}/examples"
  cp -r "${code_dir}/examples/c" "${server_dir}/examples/"
fi

# --- Outer package: install.sh ---
cp "${tools_dir}/install.sh" "${server_dir}/"
sed -i 's/verMode=cluster/verMode=edge/g' "${server_dir}/install.sh" 2>/dev/null || :
patch_server_install_messages "${server_dir}/install.sh"
chmod a+x "${server_dir}/install.sh"

# --- Outer package: start-all.sh / stop-all.sh ---
# Community edition: only taosd service (no taosx/adapter/keeper/explorer)
cat > "${server_dir}/start-all.sh" << 'STARTEOF'
#!/bin/bash

prefix="taos"
versionType="community"
SERVICES=("${prefix}d")
OS_TYPE=$(uname)

start_service() {
    local service="$1"
    if [ "${OS_TYPE}" = "Linux" ]; then
        if [ "$(id -u)" -eq 0 ]; then
            systemctl start "${service}" 2>/dev/null && echo "✓ ${service} started" || echo "✗ Failed to start ${service}"
        else
            systemctl --user start "${service}" 2>/dev/null && echo "✓ ${service} started" || echo "✗ Failed to start ${service}"
        fi
    fi
}

echo "Starting TDengine Community services..."
for service in "${SERVICES[@]}"; do
    start_service "${service}"
done
echo "Done."
STARTEOF
chmod a+x "${server_dir}/start-all.sh"

cat > "${server_dir}/stop-all.sh" << 'STOPEOF'
#!/bin/bash

prefix="taos"
versionType="community"
SERVICES=("${prefix}d")
OS_TYPE=$(uname)

stop_service() {
    local service="$1"
    if [ "${OS_TYPE}" = "Linux" ]; then
        if [ "$(id -u)" -eq 0 ]; then
            systemctl stop "${service}" 2>/dev/null && echo "${service} stopped" || echo "Failed to stop ${service}"
        else
            systemctl --user stop "${service}" 2>/dev/null && echo "${service} stopped" || echo "Failed to stop ${service}"
        fi
    fi
}

echo "Stopping TDengine Community services..."
for service in "${SERVICES[@]}"; do
    stop_service "${service}"
done
echo "Done."
STOPEOF
chmod a+x "${server_dir}/stop-all.sh"

# --- Create outer tar.gz ---
echo "  Creating server tar.gz..."
cd "${release_dir}"

if [[ "$verType" == "beta" ]] || [[ "$verType" == "preRelease" ]]; then
  server_tar_name="${server_pkg_name}-${verType}-${os_type}-${arch}.tar.gz"
else
  server_tar_name="${server_pkg_name}-${os_type}-${arch}.tar.gz"
fi

tar -zcf "${server_tar_name}" "$(basename "$server_dir")" --remove-files
echo "  ✓ Server package: ${release_dir}/${server_tar_name}"

# ======================== Build Client Package ========================

echo ""
echo ">>> Building client package..."

client_pkg_name="${productName}-client-${version}"
client_dir="${release_dir}/${client_pkg_name}"

# Clean previous output
rm -rf "${client_dir}"
mkdir -p "${client_dir}"

# --- Inner package: bin/ ---
mkdir -p "${client_dir}/bin"

# Core binary
cp "${build_dir}/bin/${clientName}" "${client_dir}/bin/"

# Optional tools
copy_if_exists "${build_dir}/bin/taosBenchmark" "${client_dir}/bin/"
copy_if_exists "${build_dir}/bin/taosdump"      "${client_dir}/bin/"

# Packaging helper scripts
copy_if_exists "${tools_dir}/remove_client.sh"  "${client_dir}/bin/"
copy_if_exists "${tools_dir}/set_core.sh"       "${client_dir}/bin/"
copy_if_exists "${tools_dir}/get_client.sh"     "${client_dir}/bin/"

chmod a+x "${client_dir}/bin/"* 2>/dev/null || :

# --- Inner package: cfg/ ---
mkdir -p "${client_dir}/cfg"
copy_if_exists "${cfg_dir}/${configFile}" "${client_dir}/cfg/"

# --- Inner package: inc/ ---
mkdir -p "${client_dir}/inc"
for header in \
  "${code_dir}/include/client/taos.h" \
  "${code_dir}/include/common/taosdef.h" \
  "${code_dir}/include/util/taoserror.h" \
  "${code_dir}/include/util/tdef.h" \
  "${code_dir}/include/libs/function/taosudf.h"; do
  copy_if_exists "$header" "${client_dir}/inc/"
done

# --- Create inner package.tar.gz ---
echo "  Creating inner package.tar.gz..."
cd "${client_dir}"
tar -zcf package.tar.gz bin cfg inc --remove-files

# --- Outer package: driver/ ---
mkdir -p "${client_dir}/driver"
cp "${build_dir}/lib/libtaos.so" "${client_dir}/driver/libtaos.so.${version}"
cp "${build_dir}/lib/libtaosnative.so" "${client_dir}/driver/libtaosnative.so.${version}"
echo "${versionComp}" > "${client_dir}/driver/vercomp.txt"

# --- Outer package: examples/ ---
if [ -d "${code_dir}/examples/c" ]; then
  mkdir -p "${client_dir}/examples"
  cp -r "${code_dir}/examples/c" "${client_dir}/examples/"
fi

# --- Outer package: install_client.sh ---
cp "${tools_dir}/install_client.sh" "${client_dir}/"
sed -i 's/verMode=cluster/verMode=edge/g' "${client_dir}/install_client.sh" 2>/dev/null || :
chmod a+x "${client_dir}/install_client.sh"

# --- Create outer tar.gz ---
echo "  Creating client tar.gz..."
cd "${release_dir}"

if [[ "$verType" == "beta" ]] || [[ "$verType" == "preRelease" ]]; then
  client_tar_name="${client_pkg_name}-${verType}-${os_type}-${arch}.tar.gz"
else
  client_tar_name="${client_pkg_name}-${os_type}-${arch}.tar.gz"
fi

tar -zcf "${client_tar_name}" "$(basename "$client_dir")" --remove-files
echo "  ✓ Client package: ${release_dir}/${client_tar_name}"

# ======================== Summary ========================

echo ""
echo "============================================================"
echo "  Packaging complete!"
echo ""
echo "  Server: ${release_dir}/${server_tar_name}"
echo "  Client: ${release_dir}/${client_tar_name}"
echo ""
ls -lh "${release_dir}/${server_tar_name}" "${release_dir}/${client_tar_name}"
echo "============================================================"
