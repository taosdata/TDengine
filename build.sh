#!/bin/bash

# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# export TD_CONFIG=Debug/Release before calling this script

TD_CONFIG=${TD_CONFIG:-Debug}
USE_CONAN=${USE_CONAN:-false}

_this_file=$0

do_gen() {
  cmake -B debug -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG} \
        -DBUILD_TOOLS=true                              \
        -DBUILD_KEEPER=true                             \
        -DBUILD_HTTP=false                              \
        -DBUILD_TEST=true                               \
        -DWEBSOCKET:STRING=true                         \
        -DBUILD_DEPENDENCY_TESTS=false                  \
        -DLOCAL_REPO:STRING=${LOCAL_REPO}               \
        -DLOCAL_URL:STRING=${LOCAL_URL}                 \
        "$@"
}

# Conan-related functions
do_conan_install() {
  echo "Installing dependencies with Conan..."
  local build_type=${TD_CONFIG,,}  # Convert to lowercase
  local preset="conan-${build_type}"

  conan create conan/cppstub
  conan create conan/fast-lzma2
  conan create conan/avro-c

  # Determine options based on build configuration
  local conan_options=""
  conan_options="$conan_options -o with_test=True"
  conan_options="$conan_options -o with_uv=True"
  conan_options="$conan_options -o with_geos=True"
  conan_options="$conan_options -o with_taos_tools=True"

  conan install . \
    --output-folder=build/${preset} \
    --build=missing \
    -s build_type=${TD_CONFIG} \
    ${conan_options} \
    "$@"
}

do_conan_gen() {
  echo "Configuring build with Conan..."
  local build_type=${TD_CONFIG,,}  # Convert to lowercase
  local build_dir="build/conan-${build_type}"
  local toolchain_file="$(pwd)/${build_dir}/generators/conan_toolchain.cmake"

  if [ ! -f "${toolchain_file}" ]; then
    echo "ERROR: Toolchain file not found: ${toolchain_file}"
    echo "Please run './build.sh conan-install' first"
    return 1
  fi

  cmake -B ${build_dir} \
        -DCMAKE_TOOLCHAIN_FILE=${toolchain_file} \
        -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG} \
        -DUSE_CONAN:BOOL=ON \
        -DBUILD_TOOLS=true \
        -DBUILD_KEEPER=true \
        -DBUILD_HTTP=false \
        -DBUILD_TEST=true \
        -DWITH_UV=true \
        -DWITH_UV_TRANS=true \
        -DWEBSOCKET:STRING=true \
        -DBUILD_DEPENDENCY_TESTS=false \
        "$@"
}

do_conan_bld() {
  echo "Building with Conan..."
  local build_type=${TD_CONFIG,,}  # Convert to lowercase
  local build_dir="build/conan-${build_type}"

  JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
  echo "JOBS:${JOBS}"
  cmake --build ${build_dir} --config ${TD_CONFIG} -j${JOBS} $@
}

do_conan_test() {
  echo "Testing with Conna build artifacts..."
  local build_type=${TD_CONFIG,,}  # Convert to lowercase
  local build_dir="build/conan-${build_type}"
  ctest --test-dir ${build_dir} -C ${TD_CONFIG} --output-on-failure "$@"
}

do_conan_post_build() {
  echo "Installing TDengine with Conan..."
  local build_type=${TD_CONFIG,,}  # Convert to lowercase
  local build_dir="build/conan-${build_type}"

  JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
  echo "JOBS:${JOBS}"
  cmake --install ${build_dir} --config ${TD_CONFIG} $@
}

do_conan_install_gen_bld() {
  do_conan_install $@ &&
  do_conan_gen &&
  do_conan_bld
}

do_bld() {
  JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
  echo "JOBS:${JOBS}"
  cmake --build debug --config ${TD_CONFIG} -j${JOBS} "$@"
}

do_test() {
  ctest --test-dir debug -C ${TD_CONFIG} --output-on-failure "$@"
}

do_start() {
  if which systemctl 2>/dev/null; then
    sudo systemctl start taosd           && echo taosd started &&
    sudo systemctl start taosadapter     && echo taosadapter started
  elif which launchctl 2>/dev/null; then
    sudo launchctl start com.tdengine.taosd       && echo taosd started       &&
    sudo launchctl start com.tdengine.taosadapter && echo taosadapter started &&
    sudo launchctl start com.tdengine.taoskeeper  && echo taoskeeper started
  fi
}

do_stop() {
  if which systemctl 2>/dev/null; then
    sudo systemctl stop taosadapter && echo taosadapter stopped
    sudo systemctl stop taosd       && echo taosd stopped
  elif which launchctl 2>/dev/null; then
    sudo launchctl stop com.tdengine.taoskeeper  && echo taoskeeper stopped
    sudo launchctl stop com.tdengine.taosadapter && echo taosadapter stopped
    sudo launchctl stop com.tdengine.taosd       && echo taosd stopped
  fi
}

do_purge() {
  sudo rm -rf /var/lib/taos         # data storage
  sudo rm -rf /var/log/taos         # logs
  sudo rm -f  /usr/bin/{taos,taosd,taosadapter,udfd,taoskeeper,taosdump,taosdemo,taosBenchmark,rmtaos,taosudf}             # executables
  sudo rm -f  /usr/local/bin/{taos,taosd,taosadapter,udfd,taoskeeper,taosdump,taosdemo,taosBenchmark,rmtaos,taosudf}       # executables
  sudo rm -rf /usr/lib/{libtaos,libtaosnative,libtaosws}.*        # libraries
  sudo rm -rf /usr/local/lib/{libtaos,libtaosnative,libtaosws}.*  # libraries
  sudo rm -rf /usr/include/{taosws.h,taos.h,taosdef.h,taoserror.h,tdef.h,taosudf.h}               # header files
  sudo rm -rf /usr/local/include/{taosws.h,taos.h,taosdef.h,taoserror.h,tdef.h,taosudf.h}         # header files
  sudo rm -rf /usr/local/taos       # all stuffs
}

case $1 in
    first-try)
        shift 1
        ./build.sh gen -DBUILD_TEST=false &&
        ./build.sh bld &&
        ./build.sh install &&
        ./build.sh start &&
        taos
        ;;
    gen)
        shift 1
        do_gen "$@" &&
        echo "Generated for '${TD_CONFIG}'" &&
        echo ==Done==
        ;;
    bld)
        shift 1
        do_bld "$@" &&
        echo "Built for '${TD_CONFIG}'" &&
        echo ==Done==
        ;;
    install)
        shift 1
        do_stop
        cmake --install debug --config ${TD_CONFIG} "$@" &&
        echo "Installed for '${TD_CONFIG}'" &&
        echo "If you wanna start taosd, run like this:" &&
        echo "./build.sh start" &&
        echo ==Done==
        ;;
    test)
        shift 1 &&
        do_test "$@" &&
        echo "Tested for '${TD_CONFIG}'"
        echo ==Done==
        ;;
    start)
        shift 1
        do_start &&
        echo ==Done==
        ;;
    stop)
        shift 1
        do_stop &&
        echo ==Done==
        ;;
    purge)
        shift 1
        do_stop
        do_purge &&
        echo ==Done==
        ;;
    conan-install)
        shift 1
        do_conan_install "$@" &&
        echo "Conan dependencies installed for '${TD_CONFIG}'" &&
        echo ==Done==
        ;;
    conan-gen)
        shift 1
        do_conan_gen "$@" &&
        echo "Generated with Conan for '${TD_CONFIG}'" &&
        echo ==Done==
        ;;
    conan-bld)
        shift 1
        do_conan_bld "$@" &&
        echo "Built with Conan for '${TD_CONFIG}'" &&
        echo ==Done==
        ;;
    conan-test)
        shift 1
        do_conan_test "$@" &&
        echo "Test with Conan build artifacts for '${TD_CONFIG}'" &&
        echo ==Done==
        ;;
    conan-build-all)
        shift 1
        do_conan_install_gen_bld "$@" &&
        echo "Complete Conan build finished for '${TD_CONFIG}'" &&
        echo ==Done==
        ;;
    conan-post-build)
        shift 1
        do_conan_post_build $@ &&
          echo "Complete installation with build type ${TD_CONFIG}"
        ;;
    senarios)
        shift 1
        cat ${_this_file} | sed -n '/^# beg =========================$/,/^# end =========================$/p' | sed '1d;$d'
        ;;
    *)
        echo 'env TD_CONFIG denotes which to build for: <Debug/Release>, Debug by default'
        echo ''
        echo 'Traditional build (ExternalProject):'
        echo '  to show senarios:       ./build.sh senarios'
        echo '  to give it a first try  ./build.sh first-try'
        echo '  to generate make files: ./build.sh gen [cmake options]'
        echo '  to build:               ./build.sh bld [cmake options for --build]'
        echo '  to install:             ./build.sh install [cmake options for --install]'
        echo ''
        echo 'Conan build (NEW - faster with binary packages):'
        echo '  to install dependencies: ./build.sh conan-install'
        echo '  to configure:            ./build.sh conan-gen'
        echo '  to build:                ./build.sh conan-bld'
        echo '  to do all in one:        ./build.sh conan-build-all'
        echo ''
        echo 'Common operations:'
        echo '  to run test:            ./build.sh test [ctest options]'
        echo '  to start:               ./build.sh start'
        echo '  to stop:                ./build.sh stop'
        echo ''
        echo 'Attention!!!!!!!!!!!:'
        echo '  to purge, all the data files will be deleted too, take it at your own risk:'
        echo '                        ./build.sh purge'
        ;;
esac

