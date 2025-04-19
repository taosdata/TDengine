#!/bin/bash

# export TD_CONFIG=Debug/Release before calling this script

TD_CONFIG=${TD_CONFIG:-Debug}

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
        sudo cmake --install debug --config ${TD_CONFIG} "$@" &&
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
    *)
        echo 'env TD_CONFIG denotes which to build for: <Debug/Release>, Debug by default'
        echo ''
        echo 'to generate make files: ./build.sh gen [cmake options]'
        echo 'to build:               ./build.sh bld [cmake options for --build]'
        echo 'to install:             ./build.sh install [cmake options for --install]'
        echo 'to run test:            ./build.sh test [ctest options]'
        echo 'to start:               ./build.sh start'
        echo 'to stop:                ./build.sh stop'
        echo 'to purge:               ./build.sh purge'
        ;;
esac

