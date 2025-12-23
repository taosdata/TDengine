#!/bin/bash

# beg =========================
# senario 1: give it a try for the first time
#   ./build.sh first-try
#
# senario 2: building release version, for internal use only, or take you own risk
#            still under development
#   TD_CONFIG=Release ./build.sh gen -DVERNUMBER=xxxx ....
#   TD_CONFIG=Release ./build.sh bld
#
# senario 3: daily routine for development, after you have tried ./build.sh gen
#   ./build.sh bld
#   ./build.sh install
#   ./build.sh start
#   ./build.sh test
#
# senario 4: specify different options to build the systme
#   # set xxx of type to yyy, and configure the build system
#   ./build.sh gen -Dxxx:type=yyy
#   # what types are valid: cmake --help-command set | less
#
#   # clear the xxx out of cmake cache, and let the build system automatically choose correct value for xxx
#   ./build.sh gen -Uxxx
#
#   # combine all in one execution
#   ./build.sh gen -Dxxx:type=yyy -Uzzz -Dmmm:type=nnn
#
# senario 5: list targets of this build system
#   ./build.sh bld --target help
#
# senario 6: build specific target, taking `shell` as example:
#   ./build.sh bld --target shell
#
# senario 7: checkout specific branch/tag of taosadapter.git and build upon
#   ./build.sh gen -DTAOSADAPTER_GIT_TAG:STRING=ver-3.3.6.0
#   ./build.sh bld
#
# senario 8: build taosadapter with specific go build options, taking `-a -x` as example:
#   ./build.sh gen -DTAOSADAPTER_BUILD_OPTIONS:STRING="-a:-x"
#   ./build.sh bld
# end =========================

# export TD_CONFIG=Debug/Release before calling this script

TD_CONFIG=${TD_CONFIG:-Debug}

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
    senarios)
        shift 1
        cat ${_this_file} | sed -n '/^# beg =========================$/,/^# end =========================$/p' | sed '1d;$d'
        ;;
    *)
        echo 'env TD_CONFIG denotes which to build for: <Debug/Release>, Debug by default'
        echo ''
        echo 'to show senarios:       ./build.sh senarios'
        echo 'to give it a first try  ./build.sh first-try'
        echo 'to generate make files: ./build.sh gen [cmake options]'
        echo 'to build:               ./build.sh bld [cmake options for --build]'
        echo 'to install:             ./build.sh install [cmake options for --install]'
        echo 'to run test:            ./build.sh test [ctest options]'
        echo 'to start:               ./build.sh start'
        echo 'to stop:                ./build.sh stop'
        echo 'Attention!!!!!!!!!!!:'
        echo 'to purge, all the data files will be deleted too, take it at your own risk:'
        echo '                        ./build.sh purge'
        ;;
esac

