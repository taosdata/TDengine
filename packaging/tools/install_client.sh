#!/bin/bash
#
# This file is used to install TDengine client on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
#set -x

# -----------------------Variables definition---------------------

dataDir="/var/lib/taos"
logDir="/var/log/taos"
productName="TDengine"
installDir="/usr/local/taos"
configDir="/etc/taos"
serverName="taosd"
clientName="taos"
uninstallScript="rmtaos"
configFile="taos.cfg"
tarName="package.tar.gz"

osType=$(uname)
pagMode=full
verMode=edge

clientName2="${clientName}"
serverName2="${serverName}"
productName2="TDengine"
emailName2="taosdata.com"

benchmarkName2="${clientName2}Benchmark"
dumpName2="${clientName2}dump"
demoName2="${clientName2}demo"
uninstallScript2="rm${clientName2}"
inspect_name="${clientName2}inspect"

if [ "$osType" != "Darwin" ]; then
  script_dir=$(dirname "$(readlink -f "$0")")
else
  script_dir="$(cd "$(dirname "$0")" && pwd)"
fi

log_link_dir=""
cfg_install_dir=""
bin_link_dir=""
lib_link_dir=""
lib64_link_dir=""
inc_link_dir=""
install_main_dir=""
bin_dir=""
data_dir=""
log_dir=""
user_mode=0

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

update_flag=0

function setup_env() {
  if [ "$(id -u)" -ne 0 ]; then
    user_mode=1
    installDir="$HOME/${clientName2}"
    dataDir="${installDir}/data"
    logDir="${installDir}/log"
    configDir="${installDir}/cfg"
    bin_link_dir="$HOME/.local/bin"
    lib_link_dir="$HOME/.local/lib"
    if [ "$osType" != "Darwin" ]; then
      lib64_link_dir="$HOME/.local/lib64"
    else
      lib64_link_dir="$HOME/.local/lib"
    fi
    inc_link_dir="$HOME/.local/include"
    mkdir -p "$bin_link_dir" "$lib_link_dir" "$lib64_link_dir" "$inc_link_dir"
  else
    user_mode=0
    if [ "$osType" != "Darwin" ]; then
      bin_link_dir="/usr/bin"
      lib_link_dir="/usr/lib"
      lib64_link_dir="/usr/lib64"
      inc_link_dir="/usr/include"
    else
      bin_link_dir="/usr/local/bin"
      lib_link_dir="/usr/local/lib"
      lib64_link_dir="/usr/local/lib"
      inc_link_dir="/usr/local/include"
    fi
  fi

  data_dir=${dataDir}
  log_dir=${logDir}
  log_link_dir="${installDir}/log"
  cfg_install_dir=${configDir}
  install_main_dir="${installDir}"
  bin_dir="${installDir}/bin"
}

function check_conflicting_system_installation() {
  if [ "$user_mode" -ne 1 ]; then
    return 0
  fi

  local detected=()
  local candidate=""
  local resolved=""

  for candidate in \
    "/usr/local/taos/bin/${serverName}" \
    "/usr/local/taos/bin/${clientName}" \
    "/usr/bin/${serverName}" \
    "/usr/bin/${clientName}" \
    "/etc/systemd/system/${serverName}.service" \
    "/usr/lib/libtaos.so" \
    "/usr/lib64/libtaos.so" \
    "/usr/lib/libtaosnative.so" \
    "/usr/lib64/libtaosnative.so" \
    "/usr/lib/libtaosws.so" \
    "/usr/lib64/libtaosws.so"; do
    [ -e "$candidate" ] && detected+=("$candidate")
  done

  for candidate in "${serverName}" "${clientName}"; do
    resolved=$(command -v "$candidate" 2>/dev/null || true)
    if [ -n "$resolved" ]; then
      resolved=$(readlink -f "$resolved" 2>/dev/null || echo "$resolved")
      case "$resolved" in
        /usr/*|/usr/local/*|/opt/homebrew/*)
          detected+=("$resolved")
          ;;
      esac
    fi
  done

  if command -v rpm >/dev/null 2>&1 && rpm -q tdengine >/dev/null 2>&1; then
    detected+=("rpm:tdengine")
  fi

  if command -v dpkg >/dev/null 2>&1 && dpkg -l tdengine 2>/dev/null | grep -q '^ii'; then
    detected+=("dpkg:tdengine")
  fi

  if [ "${#detected[@]}" -gt 0 ]; then
    echo -e "${RED}A system-wide TDengine installation was detected.${NC}"
    echo "Non-root installation is blocked while a system-wide TDengine installation is present."
    echo "Please uninstall the system-wide TDengine installation as root before continuing."
    echo "Detected system-wide TDengine paths:"
    printf '  %s\n' "${detected[@]}"
    exit 1
  fi
}

function kill_client() {
  pid=$(ps -ef | grep "${clientName}" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    kill -9 "$pid" || :
  fi
}

function install_main_path() {
  if [ "$user_mode" -eq 0 ]; then
    rm -rf "${install_main_dir}" || :
  else
    rm -rf "${install_main_dir}/bin" "${install_main_dir}/driver" "${install_main_dir}/examples" "${install_main_dir}/include" "${install_main_dir}/connector" || :
  fi
  mkdir -p "${install_main_dir}"
  mkdir -p "${install_main_dir}/cfg"
  mkdir -p "${install_main_dir}/bin"
  mkdir -p "${install_main_dir}/driver"
  if [[ "$productName2" == "TDengine"* ]]; then
    mkdir -p "${install_main_dir}/examples"
  fi
  mkdir -p "${install_main_dir}/include"
  if [ "$verMode" == "cluster" ]; then
    mkdir -p "${install_main_dir}/connector"
  fi
}

function install_bin() {
  rm -f "${bin_link_dir}/${clientName}" || :
  if [ "$osType" != "Darwin" ]; then
    rm -f "${bin_link_dir}/taosdemo" || :
    rm -f "${bin_link_dir}/${inspect_name}" || :
  fi
  rm -f "${bin_link_dir}/${uninstallScript}" || :
  rm -f "${bin_link_dir}/set_core" || :
  rm -f "${bin_link_dir}/${benchmarkName2}" || :
  rm -f "${bin_link_dir}/${dumpName2}" || :

  cp -r "${script_dir}/bin/"* "${install_main_dir}/bin"
  chmod 0555 "${install_main_dir}/bin/"*

  [ -x "${install_main_dir}/bin/${clientName2}" ] && ln -sf "${install_main_dir}/bin/${clientName2}" "${bin_link_dir}/${clientName2}" || :
  if [ "$osType" != "Darwin" ]; then
    [ -x "${install_main_dir}/bin/${demoName2}" ] && ln -sf "${install_main_dir}/bin/${demoName2}" "${bin_link_dir}/${demoName2}" || :
    [ -x "${install_main_dir}/bin/${inspect_name}" ] && ln -sf "${install_main_dir}/bin/${inspect_name}" "${bin_link_dir}/${inspect_name}" || :
  fi
  [ -x "${install_main_dir}/bin/remove_client.sh" ] && ln -sf "${install_main_dir}/bin/remove_client.sh" "${bin_link_dir}/${uninstallScript}" || :
  [ -x "${install_main_dir}/bin/set_core.sh" ] && ln -sf "${install_main_dir}/bin/set_core.sh" "${bin_link_dir}/set_core" || :
  [ -x "${install_main_dir}/bin/${benchmarkName2}" ] && ln -sf "${install_main_dir}/bin/${benchmarkName2}" "${bin_link_dir}/${benchmarkName2}" || :
  [ -x "${install_main_dir}/bin/${dumpName2}" ] && ln -sf "${install_main_dir}/bin/${dumpName2}" "${bin_link_dir}/${dumpName2}" || :

  if [ "$user_mode" -eq 1 ]; then
    local env_file=""
    local login_shell="${SHELL##*/}"
    if [ "$login_shell" = "zsh" ]; then
      env_file="${HOME}/.zshrc"
    elif [ "$login_shell" = "bash" ] || [ -z "$login_shell" ]; then
      env_file="${HOME}/.bashrc"
    else
      env_file="${HOME}/.profile"
    fi

    if ! grep -q "${bin_link_dir}" "$env_file" 2>/dev/null; then
      echo -e "\n# ${productName2} install path" >>"$env_file"
      echo "export PATH=\"${bin_link_dir}:\$PATH\"" >>"$env_file"
    fi

    if ! grep -q "${lib_link_dir}" "$env_file" 2>/dev/null; then
      echo "export LD_LIBRARY_PATH=\"${lib_link_dir}:\$LD_LIBRARY_PATH\"" >>"$env_file"
    fi
  fi
}

function clean_lib() {
  if [ "$user_mode" -ne 0 ]; then
    return 0
  fi

  rm -f /usr/lib/libtaos.* || :
  rm -f /usr/lib/libtaosnative.* || :
  [ -f /usr/lib/libtaosws.so ] && rm -f /usr/lib/libtaosws.so || :
  [ -f /usr/lib64/libtaosws.so ] && rm -f /usr/lib64/libtaosws.so || :
}

function install_lib() {
  rm -f "${lib_link_dir}"/libtaos.* || :
  rm -f "${lib64_link_dir}"/libtaos.* || :
  rm -f "${lib_link_dir}"/libtaosnative.* || :
  rm -f "${lib64_link_dir}"/libtaosnative.* || :

  [ -f "${lib_link_dir}/libtaosws.so" ] && rm -f "${lib_link_dir}/libtaosws.so" || :
  [ -f "${lib64_link_dir}/libtaosws.so" ] && rm -f "${lib64_link_dir}/libtaosws.so" || :

  cp -rf "${script_dir}/driver/"* "${install_main_dir}/driver"
  chmod 777 "${install_main_dir}/driver/"*

  if [ "$osType" != "Darwin" ]; then
    ln -sf "${install_main_dir}/driver/libtaos."* "${lib_link_dir}/libtaos.so.1"
    ln -sf "${lib_link_dir}/libtaos.so.1" "${lib_link_dir}/libtaos.so"
    ln -sf "${install_main_dir}/driver/libtaosnative."* "${lib_link_dir}/libtaosnative.so.1"
    ln -sf "${lib_link_dir}/libtaosnative.so.1" "${lib_link_dir}/libtaosnative.so"

    [ -f "${install_main_dir}/driver/libtaosws.so" ] && ln -sf "${install_main_dir}/driver/libtaosws.so" "${lib_link_dir}/libtaosws.so" || :

    if [ -d "${lib64_link_dir}" ] && [ "${lib64_link_dir}" != "${lib_link_dir}" ]; then
      ln -sf "${install_main_dir}/driver/libtaos."* "${lib64_link_dir}/libtaos.so.1" || :
      ln -sf "${lib64_link_dir}/libtaos.so.1" "${lib64_link_dir}/libtaos.so" || :
      ln -sf "${install_main_dir}/driver/libtaosnative."* "${lib64_link_dir}/libtaosnative.so.1" || :
      ln -sf "${lib64_link_dir}/libtaosnative.so.1" "${lib64_link_dir}/libtaosnative.so" || :

      [ -f "${install_main_dir}/driver/libtaosws.so" ] && ln -sf "${install_main_dir}/driver/libtaosws.so" "${lib64_link_dir}/libtaosws.so" || :
    fi

    if [ "$user_mode" -eq 0 ]; then
      ldconfig
    fi
  else
    ln -sf "${install_main_dir}/driver/libtaos."* "${lib_link_dir}/libtaos.1.dylib"
    ln -sf "${lib_link_dir}/libtaos.1.dylib" "${lib_link_dir}/libtaos.dylib"
    ln -sf "${install_main_dir}/driver/libtaosnative."* "${lib_link_dir}/libtaosnative.1.dylib"
    ln -sf "${lib_link_dir}/libtaosnative.1.dylib" "${lib_link_dir}/libtaosnative.dylib"

    [ -f "${install_main_dir}/driver/libtaosws.dylib" ] && ln -sf "${install_main_dir}/driver/libtaosws.dylib" "${lib_link_dir}/libtaosws.dylib" || :

    if [ "$user_mode" -eq 0 ]; then
      update_dyld_shared_cache
    fi
  fi
}

function install_header() {
  rm -f "${inc_link_dir}/taos.h" "${inc_link_dir}/taosws.h" "${inc_link_dir}/taosdef.h" "${inc_link_dir}/tdef.h" "${inc_link_dir}/taoserror.h" "${inc_link_dir}/taosudf.h" || :
  cp -f "${script_dir}/inc/"* "${install_main_dir}/include"
  chmod 644 "${install_main_dir}/include/"*
  ln -sf "${install_main_dir}/include/taos.h" "${inc_link_dir}/taos.h"
  ln -sf "${install_main_dir}/include/taosdef.h" "${inc_link_dir}/taosdef.h"
  ln -sf "${install_main_dir}/include/tdef.h" "${inc_link_dir}/tdef.h"
  ln -sf "${install_main_dir}/include/taoserror.h" "${inc_link_dir}/taoserror.h"
  ln -sf "${install_main_dir}/include/taosudf.h" "${inc_link_dir}/taosudf.h"

  [ -f "${install_main_dir}/include/taosws.h" ] && ln -sf "${install_main_dir}/include/taosws.h" "${inc_link_dir}/taosws.h" || :
}

function install_jemalloc() {
  jemalloc_dir=${script_dir}/jemalloc

  if [ "$user_mode" -ne 0 ]; then
    return 0
  fi

  if [ -d "${jemalloc_dir}" ]; then
    /usr/bin/install -c -d /usr/local/bin

    if [ -f "${jemalloc_dir}/bin/jemalloc-config" ]; then
      /usr/bin/install -c -m 755 "${jemalloc_dir}/bin/jemalloc-config" /usr/local/bin
    fi
    if [ -f "${jemalloc_dir}/bin/jemalloc.sh" ]; then
      /usr/bin/install -c -m 755 "${jemalloc_dir}/bin/jemalloc.sh" /usr/local/bin
    fi
    if [ -f "${jemalloc_dir}/bin/jeprof" ]; then
      /usr/bin/install -c -m 755 "${jemalloc_dir}/bin/jeprof" /usr/local/bin
    fi
    if [ -f "${jemalloc_dir}/include/jemalloc/jemalloc.h" ]; then
      /usr/bin/install -c -d /usr/local/include/jemalloc
      /usr/bin/install -c -m 644 "${jemalloc_dir}/include/jemalloc/jemalloc.h" /usr/local/include/jemalloc
    fi
    if [ -f "${jemalloc_dir}/lib/libjemalloc.so.2" ]; then
      /usr/bin/install -c -d /usr/local/lib
      /usr/bin/install -c -m 755 "${jemalloc_dir}/lib/libjemalloc.so.2" /usr/local/lib
      ln -sf libjemalloc.so.2 /usr/local/lib/libjemalloc.so
      /usr/bin/install -c -d /usr/local/lib
      if [ -f "${jemalloc_dir}/lib/pkgconfig/jemalloc.pc" ]; then
        /usr/bin/install -c -d /usr/local/lib/pkgconfig
        /usr/bin/install -c -m 644 "${jemalloc_dir}/lib/pkgconfig/jemalloc.pc" /usr/local/lib/pkgconfig
      fi
    fi
    if [ -f "${jemalloc_dir}/share/doc/jemalloc/jemalloc.html" ]; then
      /usr/bin/install -c -d /usr/local/share/doc/jemalloc
      /usr/bin/install -c -m 644 "${jemalloc_dir}/share/doc/jemalloc/jemalloc.html" /usr/local/share/doc/jemalloc
    fi
    if [ -f "${jemalloc_dir}/share/man/man3/jemalloc.3" ]; then
      /usr/bin/install -c -d /usr/local/share/man/man3
      /usr/bin/install -c -m 644 "${jemalloc_dir}/share/man/man3/jemalloc.3" /usr/local/share/man/man3
    fi

    if [ -d /etc/ld.so.conf.d ]; then
      echo "/usr/local/lib" | tee /etc/ld.so.conf.d/jemalloc.conf >/dev/null || echo -e "failed to write /etc/ld.so.conf.d/jemalloc.conf"
      ldconfig
    else
      echo "/etc/ld.so.conf.d not found!"
    fi
  fi
}

function install_config() {
  file_name=${cfg_install_dir}/${configFile}
  if [ -f "${file_name}" ]; then
    echo "The configuration file ${file_name} already exists"
    cp "${file_name}" "${cfg_install_dir}/${configFile}.new"
  else
    mkdir -p "${cfg_install_dir}"
    [ -f "${script_dir}/cfg/${configFile}" ] && cp "${script_dir}/cfg/${configFile}" "${cfg_install_dir}"
    chmod 644 "${cfg_install_dir}/"* || :
  fi

  if [ "${cfg_install_dir}" != "${install_main_dir}/cfg" ]; then
    ln -sfn "${cfg_install_dir}/${configFile}" "${install_main_dir}/cfg"
  fi
}

function install_taosinspect_config() {
  file_name="${script_dir}/cfg/inspect.cfg"
  if [ -f "${file_name}" ]; then
    if [ -f "${cfg_install_dir}/inspect.cfg" ]; then
      cp "${file_name}" "${cfg_install_dir}/inspect.cfg.new"
    else
      mkdir -p "${cfg_install_dir}"
      cp "${file_name}" "${cfg_install_dir}/inspect.cfg"
    fi
  fi

  if [ "${cfg_install_dir}" != "${install_main_dir}/cfg" ]; then
    ln -sfn "${cfg_install_dir}/inspect.cfg" "${install_main_dir}/cfg"
  fi
}

function install_log() {
  rm -rf "${log_dir}" || :
  mkdir -p "${log_dir}"
  chmod 777 "${log_dir}" || :

  if [ "${log_dir}" != "${install_main_dir}/log" ]; then
    ln -sfn "${log_dir}" "${install_main_dir}/log"
  fi
}

function install_connector() {
  if [ -d "${script_dir}/connector" ]; then
    cp -rf "${script_dir}/connector/" "${install_main_dir}/"
  fi
}

function install_examples() {
  if [ -d "${script_dir}/examples" ]; then
    cp -rf "${script_dir}/examples/"* "${install_main_dir}/examples"
  fi
}

function update_TDengine() {
  if [ ! -e "${tarName}" ]; then
    echo "File ${tarName} does not exist"
    exit 1
  fi
  tar -zxf "${tarName}"
  echo -e "${GREEN}Start to update ${productName2} client...${NC}"
  if ps aux | grep -v grep | grep "${clientName2}" &> /dev/null; then
    kill_client
    sleep 1
  fi

  install_main_path
  echo "${install_main_dir}" > "${install_main_dir}/.install_path"

  install_log
  install_header
  install_lib
  install_jemalloc
  if [ "$verMode" == "cluster" ]; then
    install_connector
    install_taosinspect_config
  fi
  install_examples
  install_bin
  install_config

  echo
  echo -e "\033[44;32;1m${productName2} client is updated successfully!${NC}"

  rm -rf $(tar -tf "${tarName}" | grep -Ev "^\./$|^\/")
}

function install_TDengine() {
  if [ ! -e "${tarName}" ]; then
    echo "File ${tarName} does not exist"
    exit 1
  fi
  tar -zxf "${tarName}"
  echo -e "${GREEN}Start to install ${productName2} client...${NC}"

  install_main_path
  echo "${install_main_dir}" > "${install_main_dir}/.install_path"

  install_log
  install_header
  install_lib
  install_jemalloc
  if [ "$verMode" == "cluster" ]; then
    install_connector
    install_taosinspect_config
  fi
  install_examples
  install_bin
  install_config

  echo
  echo -e "\033[44;32;1m${productName2} client is installed successfully!${NC}"

  rm -rf $(tar -tf "${tarName}" | grep -Ev "^\./$|^\/")
}

setup_env
check_conflicting_system_installation

## ==============================Main program starts from here============================
# Install or updata client and client
# if server is already install, don't install client
if [ -e "${bin_dir}/${serverName}" ]; then
  echo -e "\033[44;32;1mThere are already installed ${productName2} server, so don't need install client!${NC}"
  exit 0
fi

if [ -x "${bin_dir}/${clientName}" ]; then
  update_flag=1
  update_TDengine
else
  install_TDengine
fi
