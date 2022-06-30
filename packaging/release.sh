#!/bin/bash
#
# Generate the deb package for ubuntu, or rpm package for centos, or tar.gz package for other linux os

set -e
#set -x

# release.sh  -v [cluster | edge]
#             -c [aarch32 | aarch64 | x64 | x86 | mips64 ...]
#             -o [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | Ningsi60 | Ningsi80 |...]
#             -V [stable | beta]
#             -l [full | lite]
#             -s [static | dynamic]
#             -d [taos | ...]
#             -n [2.0.0.3]
#             -m [2.0.0.0]
#             -H [ false | true]

# set parameters by default value
verMode=edge    # [cluster, edge]
verType=stable  # [stable, beta]
cpuType=x64     # [aarch32 | aarch64 | x64 | x86 | mips64 ...]
osType=Linux    # [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | Ningsi60 | Ningsi80 |...]
pagMode=full    # [full | lite]
soMode=dynamic  # [static | dynamic]
dbName=taos     # [taos | ...]
allocator=glibc # [glibc | jemalloc]
verNumber=""
verNumberComp="2.0.0.0"
httpdBuild=false

while getopts "hv:V:c:o:l:s:d:a:n:m:H:" arg; do
  case $arg in
  v)
    #echo "verMode=$OPTARG"
    verMode=$(echo $OPTARG)
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
    echo "Usage: $(basename $0) -v [cluster | edge] "
    echo "                  -c [aarch32 | aarch64 | x64 | x86 | mips64 ...] "
    echo "                  -o [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | Ningsi60 | Ningsi80 |...] "
    echo "                  -V [stable | beta] "
    echo "                  -l [full | lite] "
    echo "                  -a [glibc | jemalloc] "
    echo "                  -s [static | dynamic] "
    echo "                  -d [taos | ...] "
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

if [ "$osType" == "Darwin" ]; then
  script_dir=$(dirname $0)
  cd ${script_dir}
  script_dir="$(pwd)"
  top_dir=${script_dir}/..
else
  script_dir="$(dirname $(readlink -f $0))"
  top_dir="$(readlink -f ${script_dir}/..)"
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

function vercomp() {
  if [[ $1 == $2 ]]; then
    echo 0
    exit 0
  fi

  local IFS=.
  local i ver1=($1) ver2=($2)

  # fill empty fields in ver1 with zeros
  for ((i = ${#ver1[@]}; i < ${#ver2[@]}; i++)); do
    ver1[i]=0
  done

  for ((i = 0; i < ${#ver1[@]}; i++)); do
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
if ( (! is_valid_version $verNumber) || (! is_valid_version $verNumberComp) || [[ "$(vercomp $verNumber $verNumberComp)" == '2' ]]); then
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

cd "${curr_dir}"

# 2. cmake executable file
compile_dir="${top_dir}/debug"
if [ -d ${compile_dir} ]; then
  rm -rf ${compile_dir}
fi

mkdir -p ${compile_dir}
cd ${compile_dir}

if [[ "$allocator" == "jemalloc" ]]; then
  allocator_macro="-DJEMALLOC_ENABLED=true"
else
  allocator_macro=""
fi

if [[ "$dbName" != "taos" ]]; then
  source ${enterprise_dir}/packaging/oem/sed_$dbName.sh
  replace_community_$dbName
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
if [[ "$cpuType" == "x64" ]] || [[ "$cpuType" == "aarch64" ]] || [[ "$cpuType" == "aarch32" ]] || [[ "$cpuType" == "mips64" ]]; then
  if [ "$verMode" != "cluster" ]; then
    # community-version compile
    cmake ../ -DCPUTYPE=${cpuType} -DOSTYPE=${osType} -DSOMODE=${soMode} -DDBNAME=${dbName} -DVERTYPE=${verType} -DVERDATE="${build_time}" -DGITINFO=${gitinfo} -DGITINFOI=${gitinfoOfInternal} -DVERNUMBER=${verNumber} -DVERCOMPATIBLE=${verNumberComp} -DPAGMODE=${pagMode} -DBUILD_HTTP=${BUILD_HTTP} -DBUILD_TOOLS=${BUILD_TOOLS} ${allocator_macro}
  else
    if [[ "$dbName" != "taos" ]]; then
      replace_enterprise_$dbName
    fi
    cmake ../../ -DCPUTYPE=${cpuType} -DOSTYPE=${osType} -DSOMODE=${soMode} -DDBNAME=${dbName} -DVERTYPE=${verType} -DVERDATE="${build_time}" -DGITINFO=${gitinfo} -DGITINFOI=${gitinfoOfInternal} -DVERNUMBER=${verNumber} -DVERCOMPATIBLE=${verNumberComp} -DBUILD_HTTP=${BUILD_HTTP} -DBUILD_TOOLS=${BUILD_TOOLS} ${allocator_macro}
  fi
else
  echo "input cpuType=${cpuType} error!!!"
  exit 1
fi

CORES=$(grep -c ^processor /proc/cpuinfo)

if [[ "$allocator" == "jemalloc" ]]; then
  # jemalloc need compile first, so disable parallel build
  make -j ${CORES} && ${csudo}make install
else
  make -j ${CORES} && ${csudo}make install
fi

cd ${curr_dir}

# 3. Call the corresponding script for packaging
if [ "$osType" != "Darwin" ]; then
  if [[ "$verMode" != "cluster" ]] && [[ "$pagMode" == "full" ]] && [[ "$cpuType" == "x64" ]] && [[ "$dbName" == "taos" ]]; then
    ret='0'
    command -v dpkg >/dev/null 2>&1 || { ret='1'; }
    if [ "$ret" -eq 0 ]; then
      echo "====do deb package for the ubuntu system===="
      output_dir="${top_dir}/debs"
      if [ -d ${output_dir} ]; then
        rm -rf ${output_dir}
      fi
      mkdir -p ${output_dir}
      cd ${script_dir}/deb
      ${csudo}./makedeb.sh ${compile_dir} ${output_dir} ${verNumber} ${cpuType} ${osType} ${verMode} ${verType}

      if [[ "$pagMode" == "full" ]]; then
        if [ -d ${top_dir}/src/kit/taos-tools/packaging/deb ]; then
          cd ${top_dir}/src/kit/taos-tools/packaging/deb
          taos_tools_ver=$(git describe --tags | sed -e 's/ver-//g' | awk -F '-' '{print $1}')
          [ -z "$taos_tools_ver" ] && taos_tools_ver="0.1.0"

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
        rm -rf ${output_dir}
      fi
      mkdir -p ${output_dir}
      cd ${script_dir}/rpm
      ${csudo}./makerpm.sh ${compile_dir} ${output_dir} ${verNumber} ${cpuType} ${osType} ${verMode} ${verType}

      if [[ "$pagMode" == "full" ]]; then
        if [ -d ${top_dir}/src/kit/taos-tools/packaging/rpm ]; then
          cd ${top_dir}/src/kit/taos-tools/packaging/rpm
          taos_tools_ver=$(git describe --tags | sed -e 's/ver-//g' | awk -F '-' '{print $1}' | sed -e 's/-/_/g')
          [ -z "$taos_tools_ver" ] && taos_tools_ver="0.1.0"

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

  ${csudo}./makepkg.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${verNumberComp} ${dbName}
  ${csudo}./makeclient.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName}
  ${csudo}./makearbi.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}

else
  # only make client for Darwin
  cd ${script_dir}/tools
  ./makeclient.sh ${compile_dir} ${verNumber} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode} ${dbName}
fi
