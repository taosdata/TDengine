#!/bin/bash
#
# Generate the deb package for ubuntu, or rpm package for centos, or tar.gz package for other linux os

set -e
set -x

# release.sh  -v [cluster | edge]
#             -c [aarch32 | aarch64 | x64 | x86 | mips64 ...]
#             -o [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | Ningsi60 | Ningsi80 |...]
#             -V [stable | beta]
#             -l [full | lite]
#             -s [static | dynamic]
#             -d [taos | power | tq | pro | kh | jh]
#             -n [2.0.0.3]
#             -m [2.0.0.0]
#             -H [ false | true]

# set parameters by default value
verMode=edge     # [cluster, edge]
verType=stable   # [stable, beta]
cpuType=x64      # [aarch32 | aarch64 | x64 | x86 | mips64 ...]
osType=Linux     # [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | Ningsi60 | Ningsi80 |...]
pagMode=full     # [full | lite]
soMode=dynamic   # [static | dynamic]
dbName=taos      # [taos | power | tq | pro | kh | jh]
allocator=glibc  # [glibc | jemalloc]
verNumber=""
verNumberComp="1.0.0.0"
httpdBuild=false

while getopts "hv:V:c:o:l:s:d:a:n:m:H:" arg
do
  case $arg in
    v)
      #echo "verMode=$OPTARG"
      verMode=$( echo $OPTARG )
      ;;
    V)
      #echo "verType=$OPTARG"
      verType=$(echo $OPTARG)
      ;;
    c)
      #echo "cpuType=$OPTARG"
      cpuType=$(echo $OPTARG)
      ;;
    l)
      #echo "pagMode=$OPTARG"
      pagMode=$(echo $OPTARG)
      ;;
    s)
      #echo "soMode=$OPTARG"
      soMode=$(echo $OPTARG)
      ;;
    d)
      #echo "dbName=$OPTARG"
      dbName=$(echo $OPTARG)
      ;;
    a)
      #echo "allocator=$OPTARG"
      allocator=$(echo $OPTARG)
      ;;
    n)
      #echo "verNumber=$OPTARG"
      verNumber=$(echo $OPTARG)
      ;;
    m)
      #echo "verNumberComp=$OPTARG"
      verNumberComp=$(echo $OPTARG)
      ;;
    o)
      #echo "osType=$OPTARG"
      osType=$(echo $OPTARG)
      ;;
    H)
      #echo "httpdBuild=$OPTARG"
      httpdBuild=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -v [cluster | edge] "
      echo "                  -c [aarch32 | aarch64 | x64 | x86 | mips64 ...] "
      echo "                  -o [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | Ningsi60 | Ningsi80 |...] "
      echo "                  -V [stable | beta] "
      echo "                  -l [full | lite] "
      echo "                  -a [glibc | jemalloc] "
      echo "                  -s [static | dynamic] "
      echo "                  -d [taos | power | tq | pro | kh | jh] "
      echo "                  -n [version number] "
      echo "                  -m [compatible version number] "
      echo "                  -H [false | true] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "verMode=${verMode} verType=${verType} cpuType=${cpuType} osType=${osType} pagMode=${pagMode} soMode=${soMode} dbName=${dbName} allocator=${allocator} verNumber=${verNumber} verNumberComp=${verNumberComp} httpdBuild=${httpdBuild}"

curr_dir=$(pwd)

if [ "$osType" != "Darwin" ]; then
  script_dir="$(dirname $(readlink -f $0))"
  top_dir="$(readlink -f ${script_dir}/..)"
else
  script_dir=`dirname $0`
  cd ${script_dir}
  script_dir="$(pwd)"
  top_dir=${script_dir}/..
fi

csudo=""
#if command -v sudo > /dev/null; then
#  csudo="sudo "
#fi

function is_valid_version() {
  [ -z $1 ] && return 1 || :

  rx='^([0-9]+\.){3}(\*|[0-9]+)$'
  if [[ $1 =~ $rx ]]; then
    return 0
  fi
  return 1
}

function vercomp () {
  if [[ $1 == $2 ]]; then
    echo 0
    exit 0
  fi

  local IFS=.
  local i ver1=($1) ver2=($2)

  # fill empty fields in ver1 with zeros
  for ((i=${#ver1[@]}; i<${#ver2[@]}; i++)); do
    ver1[i]=0
  done

  for ((i=0; i<${#ver1[@]}; i++)); do
    if [[ -z ${ver2[i]} ]]; then
      # fill empty fields in ver2 with zeros
      ver2[i]=0
    fi
    if ((10#${ver1[i]} > 10#${ver2[i]})); then
      echo 1
      exit 0
    fi
    if ((10#${ver1[i]} < 10#${ver2[i]})); then
      echo 2
      exit 0
    fi
  done
  echo 0
}

# 1. check version information
if ( ( ! is_valid_version $verNumber ) || ( ! is_valid_version $verNumberComp ) || [[ "$(vercomp $verNumber $verNumberComp)" == '2' ]] ); then
  echo "please enter correct version"
  exit 0
fi

echo "=======================new version number: ${verNumber}, compatible version: ${verNumberComp}======================================"

build_time=$(date +"%F %R")

# get commint id from git
gitinfo=$(git rev-parse --verify HEAD)

if [[ "$verMode" == "cluster" ]]; then
  enterprise_dir="${top_dir}/../enterprise"
  cd ${enterprise_dir}
  gitinfoOfInternal=$(git rev-parse --verify HEAD)
else
  gitinfoOfInternal=NULL
fi

cd ${curr_dir}

# 2. cmake executable file
compile_dir="${top_dir}/debug"
if [ -d ${compile_dir} ]; then
  ${csudo}rm -rf ${compile_dir}
fi

if [ "$osType" != "Darwin" ]; then
  ${csudo}mkdir -p ${compile_dir}
else
  mkdir -p ${compile_dir}
fi
cd ${compile_dir}

if [[ "$allocator" == "jemalloc" ]]; then
    allocator_macro="-DJEMALLOC_ENABLED=true"
else
    allocator_macro=""
fi

# for powerdb
if [[ "$dbName" == "power" ]]; then
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/PowerDB/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/power\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/powerdemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/power_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"powerdb\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos config/power config/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/taos\.cfg/power\.cfg/g"   ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/power/g"   ${top_dir}/src/util/src/tconfig.c
  # src/kit/taosdemo/taosdemo.c
  sed -i "s/\"taosdata\"/\"powerdb\"/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/power/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/Power/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/Power/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/powerdlog/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/powerlog/g"   ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/powerinfo/g"   ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/power\.cfg/g"   ${top_dir}/src/dnode/CMakeLists.txt
  # src/kit/taosdump/taosdump.c
  sed -i "s/TDengine/Power/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/Default is taosdata/Default is power/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/\"taosdata\"/\"powerdb\"/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/taos\/taos\.cfg/power\/power\.cfg/g" ${top_dir}/src/kit/taosdump/taosdump.c
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/power/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/power/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/power/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  # src/os/src/windows/wEnv.c
  sed -i "s/TDengine/PowerDB/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/PowerDB shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2020 by PowerDB, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"power> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\"    -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 7/g" ${top_dir}/src/kit/shell/src/shellEngine.c
fi

# for tq
if [[ "$dbName" == "tq" ]]; then
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/TQueue/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/tq\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/tqdemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/tq_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos config/tq config/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/taos\.cfg/tq\.cfg/g"   ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/tq/g"   ${top_dir}/src/util/src/tconfig.c
  # src/kit/taosdemo/taosdemo.c
  sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/tq/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/TQueue/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/TQueue/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/tqdlog/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/tqlog/g"   ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/tqinfo/g"   ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/tq\.cfg/g"   ${top_dir}/src/dnode/CMakeLists.txt
  # src/kit/taosdump/taosdump.c
  sed -i "s/TDengine/TQueue/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/Default is taosdata/Default is tqueue/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/taos\/taos\.cfg/tq\/tq\.cfg/g" ${top_dir}/src/kit/taosdump/taosdump.c
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/tq/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/tq/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/tq/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  # src/os/src/windows/wEnv.c
  sed -i "s/TDengine/TQ/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/TQ shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2020 by TQ, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"tq> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\" -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 4/g" ${top_dir}/src/kit/shell/src/shellEngine.c
fi

# for prodb
if [[ "$dbName" == "pro" ]]; then
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/ProDB/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/prodbc\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/prodemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/prodb_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos config/prodb config/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/taos\.cfg/prodb\.cfg/g"   ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/ProDB/g"   ${top_dir}/src/util/src/tconfig.c
  # src/kit/taosdemo/taosdemo.c
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/support@taosdata.com/support@hanatech.com.cn/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/ProDB/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/ProDB/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/prodlog/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/prolog/g"   ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/proinfo/g"   ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/prodb\.cfg/g"   ${top_dir}/src/dnode/CMakeLists.txt
  # src/kit/taosdump/taosdump.c
  sed -i "s/Default is taosdata/Default is prodb/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/taos\/taos\.cfg/ProDB\/prodb\.cfg/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/kit/taosdump/taosdump.c
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/ProDB/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/ProDB/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/ProDB/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  # src/os/src/windows/wEnv.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/ProDB shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2020 by Hanatech, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"ProDB> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\"    -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 7/g" ${top_dir}/src/kit/shell/src/shellEngine.c
fi

# for KingHistorian
if [[ "$dbName" == "kh" ]]; then
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/KingHistorian/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/khclient\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/khdemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/kh_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos config/kh config/g"  ${top_dir}/src/util/src/tconfig.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/kinghistorian/g"   ${top_dir}/src/util/src/tconfig.c
  # src/kit/taosdemo/taosdemo.c
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/support@taosdata.com/support@wellintech.com/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/kinghistorian/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/KingHistorian/g"  ${top_dir}/src/dnode/src/dnodeSystem.c 
  sed -i "s/TDengine/KingHistorian/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/khserverlog/g"  ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/khclientlog/g"  ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/khinfo/g"  ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/src/dnode/CMakeLists.txt
  # src/dnode/CMakeLists.txt
  sed -i "s/Default is taosdata/Default is khroot/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/taos\/taos\.cfg/kinghistorian\/kinghistorian\.cfg/g" ${top_dir}/src/kit/taosdump/taosdump.c
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/kinghistorian/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/kinghistorian/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/kinghistorian/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  # src/os/src/windows/wEnv.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/KingHistorian shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2021 by Wellintech, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"kh> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\" -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 4/g" ${top_dir}/src/kit/shell/src/shellEngine.c
fi

# for jinheng
if [[ "$dbName" == "jh" ]]; then
  # Following files to change:
  # * src/client/src/tscSystem.c
  # * src/inc/taosdef.h
  # * src/kit/shell/CMakeLists.txt
  # * src/kit/shell/inc/shell.h
  # * src/kit/shell/src/shellEngine.c
  # * src/kit/shell/src/shellWindows.c
  # * src/kit/taosdemo/taosdemo.c
  # * src/kit/taosdump/taosdump.c
  # * src/os/src/linux/linuxEnv.c
  # * src/os/src/windows/wEnv.c
  # * src/util/src/tconfig.c
  # * src/util/src/tlog.c

  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/jh_iot/g"  ${top_dir}/src/dnode/src/dnodeSystem.c 
  # src/dnode/src/dnodeMain.c
  sed -i "s/TDengine/jh_iot/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  # TODO: src/dnode/CMakeLists.txt
fi

if [[ "$httpdBuild" == "true" ]]; then
    BUILD_HTTP=true
else
    BUILD_HTTP=false
fi

if [[ "$verMode" == "cluster" ]]; then
    BUILD_HTTP=internal
fi

if [[ "$pagMode" == "full" ]]; then
    BUILD_TOOLS=true
else
    BUILD_TOOLS=false
fi

# check support cpu type
if [[ "$cpuType" == "x64" ]] || [[ "$cpuType" == "aarch64" ]] || [[ "$cpuType" == "aarch32" ]] || [[ "$cpuType" == "mips64" ]] ; then
  if [ "$verMode" != "cluster" ]; then
    # community-version compile
    cmake ../    -DCPUTYPE=${cpuType} -DOSTYPE=${osType} -DSOMODE=${soMode} -DDBNAME=${dbName} -DVERTYPE=${verType} -DVERDATE="${build_time}" -DGITINFO=${gitinfo} -DGITINFOI=${gitinfoOfInternal} -DVERNUMBER=${verNumber} -DVERCOMPATIBLE=${verNumberComp} -DPAGMODE=${pagMode} -DBUILD_HTTP=${BUILD_HTTP} -DBUILD_TOOLS=${BUILD_TOOLS} ${allocator_macro}
  else
    # enterprise-version compile
    if [[ "$dbName" == "power" ]]; then
      # enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/\"taosdata\"/\"powerdb\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/TDengine/PowerDB/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      # enterprise/src/plugins/admin/src/httpAdminHandle.c
      sed -i "s/taos\.cfg/power\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
      # enterprise/src/plugins/grant/src/grantMain.c
      sed -i "s/taos\.cfg/power\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
      # enterprise/src/plugins/module/src/moduleMain.c
      sed -i "s/taos\.cfg/power\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
    fi
    if [[ "$dbName" == "tq" ]]; then
      # enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/TDengine/TQueue/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      # enterprise/src/plugins/admin/src/httpAdminHandle.c
      sed -i "s/taos\.cfg/tq\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
      # enterprise/src/plugins/grant/src/grantMain.c
      sed -i "s/taos\.cfg/tq\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
      # enterprise/src/plugins/module/src/moduleMain.c
      sed -i "s/taos\.cfg/tq\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
    fi
    if [[ "$dbName" == "pro" ]]; then
      # enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/TDengine/ProDB/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      # enterprise/src/plugins/admin/src/httpAdminHandle.c
      sed -i "s/taos\.cfg/prodb\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
      # enterprise/src/plugins/grant/src/grantMain.c
      sed -i "s/taos\.cfg/prodb\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
      # enterprise/src/plugins/module/src/moduleMain.c
      sed -i "s/taos\.cfg/prodb\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
    fi
    if [[ "$dbName" == "kh" ]]; then
      # enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/TDengine/KingHistorian/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      # enterprise/src/plugins/admin/src/httpAdminHandle.c
      sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
      # enterprise/src/plugins/grant/src/grantMain.c
      sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
      # enterprise/src/plugins/module/src/moduleMain.c
      sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
    fi
    if [[ "$dbName" == "jh" ]]; then
      # enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/\"taosdata\"/\"jhdata\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      sed -i "s/TDengine/jh_iot/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
      # enterprise/src/plugins/admin/src/httpAdminHandle.c
      #sed -i "s/taos\.cfg/taos\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
      # enterprise/src/plugins/grant/src/grantMain.c
      #sed -i "s/taos\.cfg/taos\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
      # enterprise/src/plugins/module/src/moduleMain.c
      #sed -i "s/taos\.cfg/taos\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
    fi

    cmake ../../ -DCPUTYPE=${cpuType} -DOSTYPE=${osType} -DSOMODE=${soMode} -DDBNAME=${dbName} -DVERTYPE=${verType} -DVERDATE="${build_time}" -DGITINFO=${gitinfo} -DGITINFOI=${gitinfoOfInternal} -DVERNUMBER=${verNumber} -DVERCOMPATIBLE=${verNumberComp} -DBUILD_HTTP=${BUILD_HTTP} -DBUILD_TOOLS=${BUILD_TOOLS} ${allocator_macro}
  fi
else
  echo "input cpuType=${cpuType} error!!!"
  exit 1
fi

CORES=`grep -c ^processor /proc/cpuinfo`

if [[ "$allocator" == "jemalloc" ]]; then
    # jemalloc need compile first, so disable parallel build
    make -j ${CORES} && ${csudo}make install
else
    make -j ${CORES} && ${csudo}make install
fi

cd ${curr_dir}

# 3. Call the corresponding script for packaging
if [ "$osType" != "Darwin" ]; then
  if [[ "$verMode" != "cluster" ]] && [[ "$cpuType" == "x64" ]] && [[ "$dbName" == "taos" ]]; then
    ret='0'
    command -v dpkg >/dev/null 2>&1 || { ret='1'; }
    if [ "$ret" -eq 0 ]; then
      echo "====do deb package for the ubuntu system===="
      output_dir="${top_dir}/debs"
      if [ -d ${output_dir} ]; then
        ${csudo}rm -rf ${output_dir}
      fi
      ${csudo}mkdir -p ${output_dir}
      cd ${script_dir}/deb
      ${csudo}./makedeb.sh ${compile_dir} ${output_dir} ${verNumber} ${cpuType} ${osType} ${verMode} ${verType}

      if [[ "$pagMode" == "full" ]]; then
          if [ -d ${top_dir}/src/kit/taos-tools/packaging/deb ]; then
              cd ${top_dir}/src/kit/taos-tools/packaging/deb
              [ -z "$taos_tools_ver" ] && taos_tools_ver="0.1.0"

              taos_tools_ver=$(git describe --tags|sed -e 's/ver-//g'|awk -F '-' '{print $1}')
              ${csudo}./make-taos-tools-deb.sh ${top_dir} \
                  ${compile_dir} ${output_dir} ${taos_tools_ver} ${cpuType} ${osType} ${verMode} ${verType}
          fi
      fi
  else
      echo "==========dpkg command not exist, so not release deb package!!!"
    fi
    ret='0'
    command -v rpmbuild >/dev/null 2>&1 || { ret='1'; }
    if [ "$ret" -eq 0 ]; then
      echo "====do rpm package for the centos system===="
      output_dir="${top_dir}/rpms"
      if [ -d ${output_dir} ]; then
        ${csudo}rm -rf ${output_dir}
      fi
      ${csudo}mkdir -p ${output_dir}
      cd ${script_dir}/rpm
      ${csudo}./makerpm.sh ${compile_dir} ${output_dir} ${verNumber} ${cpuType} ${osType} ${verMode} ${verType}

      if [[ "$pagMode" == "full" ]]; then
          if [ -d ${top_dir}/src/kit/taos-tools/packaging/rpm ]; then
              cd ${top_dir}/src/kit/taos-tools/packaging/rpm
              [ -z "$taos_tools_ver" ] && taos_tools_ver="0.1.0"

              taos_tools_ver=$(git describe --tags|sed -e 's/ver-//g'|awk -F '-' '{print $1}'|sed -e 's/-/_/g')
              ${csudo}./make-taos-tools-rpm.sh ${top_dir} \
                  ${compile_dir} ${output_dir} ${taos_tools_ver} ${cpuType} ${osType} ${verMode} ${verType}
          fi
      fi
  else
      echo "==========rpmbuild command not exist, so not release rpm package!!!"
    fi
  fi

  echo "====do tar.gz package for all systems===="
  cd ${script_dir}/tools

  if [[ "$dbName" == "taos" ]]; then
    ${csudo}./makepkg.sh    ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${verNumberComp}
    ${csudo}./makeclient.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
    ${csudo}./makearbi.sh   ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
  elif [[ "$dbName" == "tq" ]]; then
    ${csudo}./makepkg_tq.sh    ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName} ${verNumberComp}
    ${csudo}./makeclient_tq.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName}
    ${csudo}./makearbi_tq.sh   ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
  elif [[ "$dbName" == "pro" ]]; then
    ${csudo}./makepkg_pro.sh    ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName} ${verNumberComp}
    ${csudo}./makeclient_pro.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName}
    ${csudo}./makearbi_pro.sh   ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
  elif [[ "$dbName" == "kh" ]]; then
    ${csudo}./makepkg_kh.sh    ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName} ${verNumberComp}
    ${csudo}./makeclient_kh.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName}
    ${csudo}./makearbi_kh.sh   ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
  elif [[ "$dbName" == "jh" ]]; then
    ${csudo}./makepkg_jh.sh    ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName} ${verNumberComp}
    ${csudo}./makeclient_jh.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName}
    ${csudo}./makearbi_jh.sh   ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
  else
    ${csudo}./makepkg_power.sh    ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName} ${verNumberComp}
    ${csudo}./makeclient_power.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName}
    ${csudo}./makearbi_power.sh   ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
  fi
else
  # only make client for Darwin
  cd ${script_dir}/tools
  ./makeclient.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${dbName}
fi
