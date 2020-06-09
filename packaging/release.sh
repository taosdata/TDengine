#!/bin/bash
#
# Generate the deb package for ubunt, or rpm package for centos, or tar.gz package for other linux os

set -e
#set -x

# releash.sh  -v [cluster | edge]  
#             -c [aarch32 | aarch64 | x64 | x86 | mips64 ...] 
#             -o [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | ...]  
#             -V [stable | beta]
#             -l [full | lite]

# set parameters by default value
verMode=edge     # [cluster, edge]
verType=stable   # [stable, beta]
cpuType=x64      # [aarch32 | aarch64 | x64 | x86 | mips64 ...]
osType=Linux     # [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | ...]
pagMode=full     # [full | lite]

while getopts "hv:V:c:o:l:" arg
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
    o)
      #echo "osType=$OPTARG"
      osType=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -v [cluster | edge]  -c [aarch32 | aarch64 | x64 | x86 | mips64 ...] -o [Linux | Kylin | Alpine | Raspberrypi | Darwin | Windows | ...]  -V [stable | beta] -l [full | lite]"
      exit 0
      ;;
    ?) #unknow option 
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "verMode=${verMode} verType=${verType} cpuType=${cpuType} osType=${osType} pagMode=${pagMode}"

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

versioninfo="${top_dir}/src/util/src/version.c"

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi

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
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            echo 1
            exit 0
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            echo 2
            exit 0
        fi
    done
    echo 0
}

# 1. Read version information
version=$(cat ${versioninfo} | grep " version" | cut -d '"' -f2)
compatible_version=$(cat ${versioninfo} | grep " compatible_version" | cut -d '"' -f2)

while true; do
  read -p "Do you want to release a new version? [y/N]: " is_version_change

  if [[ ( "${is_version_change}" == "y") || ( "${is_version_change}" == "Y") ]]; then
      read -p "Please enter the new version: " tversion
      while true; do
          if (! is_valid_version $tversion) || [ "$(vercomp $tversion $version)" = '2' ]; then
              read -p "Please enter a correct version: " tversion
              continue
          fi
          version=${tversion}
          break
      done

      echo

      read -p "Enter the oldest compatible version: " tversion
      while true; do

          if [ -z $tversion ]; then
              break
          fi

          if (! is_valid_version $tversion) || [ "$(vercomp $version $tversion)" = '2' ]; then
              read -p "enter correct compatible version: " tversion
          else
              compatible_version=$tversion
              break
          fi
      done

      break
  elif [[ ( "${is_version_change}" == "n") || ( "${is_version_change}" == "N") ]]; then
      echo "Use old version: ${version} compatible version: ${compatible_version}."
      break
  else
      continue
  fi
done

# output the version info to the buildinfo file.
build_time=$(date +"%F %R")
echo "char version[12] = \"${version}\";"                             > ${versioninfo}
echo "char compatible_version[12] = \"${compatible_version}\";"      >> ${versioninfo}
echo "char gitinfo[48] = \"$(git rev-parse --verify HEAD)\";"       >> ${versioninfo}
if [ "$verMode" != "cluster" ]; then
  echo "char gitinfoOfInternal[48] = \"\";"                         >> ${versioninfo}
else
  enterprise_dir="${top_dir}/../enterprise"
  cd ${enterprise_dir}
  echo "char gitinfoOfInternal[48] = \"$(git rev-parse --verify HEAD)\";"  >> ${versioninfo}
  cd ${curr_dir}
fi
echo "char buildinfo[64] = \"Built by ${USER} at ${build_time}\";"  >> ${versioninfo}
echo ""                                                              >> ${versioninfo}
tmp_version=$(echo $version | tr -s "." "_")
if [ "$verMode" == "cluster" ]; then
  libtaos_info=${tmp_version}_${osType}_${cpuType}
else
  libtaos_info=edge_${tmp_version}_${osType}_${cpuType}
fi
if [ "$verType" == "beta" ]; then
  libtaos_info=${libtaos_info}_${verType}
fi
echo "void libtaos_${libtaos_info}() {};"        >> ${versioninfo}

# 2. cmake executable file
compile_dir="${top_dir}/debug"
if [ -d ${compile_dir} ]; then
    ${csudo} rm -rf ${compile_dir}
fi

if [ "$osType" != "Darwin" ]; then
    ${csudo} mkdir -p ${compile_dir}
else
    mkdir -p ${compile_dir}
fi
cd ${compile_dir}

# check support cpu type
if [[ "$cpuType" == "x64" ]] || [[ "$cpuType" == "aarch64" ]] || [[ "$cpuType" == "aarch32" ]] || [[ "$cpuType" == "mips64" ]] ; then
    if [ "$verMode" != "cluster" ]; then
      cmake ../ -DCPUTYPE=${cpuType} -DPAGMODE=${pagMode}
    else
      cmake ../../ -DCPUTYPE=${cpuType}
    fi
else
    echo "input cpuType=${cpuType} error!!!"
    exit 1
fi

make

cd ${curr_dir}

# 3. judge the operating system type, then Call the corresponding script for packaging
#osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
#osinfo=$(cat /etc/os-release | grep "NAME" | cut -d '"' -f2)
#echo "osinfo: ${osinfo}"

if [ "$osType" != "Darwin" ]; then
    if [[ "$verMode" != "cluster" ]] && [[ "$cpuType" == "x64" ]]; then
        echo "====do deb package for the ubuntu system===="
        output_dir="${top_dir}/debs"
        if [ -d ${output_dir} ]; then
            ${csudo} rm -rf ${output_dir}
        fi
        ${csudo} mkdir -p ${output_dir}
        cd ${script_dir}/deb
        ${csudo} ./makedeb.sh ${compile_dir} ${output_dir} ${version} ${cpuType} ${osType} ${verMode} ${verType}

        echo "====do rpm package for the centos system===="
        output_dir="${top_dir}/rpms"
        if [ -d ${output_dir} ]; then
            ${csudo} rm -rf ${output_dir}
        fi
        ${csudo} mkdir -p ${output_dir}
        cd ${script_dir}/rpm
        ${csudo} ./makerpm.sh ${compile_dir} ${output_dir} ${version} ${cpuType} ${osType} ${verMode} ${verType}
    fi
	
    echo "====do tar.gz package for all systems===="
    cd ${script_dir}/tools
    
	${csudo} ./makepkg.sh    ${compile_dir} ${version} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
	${csudo} ./makeclient.sh ${compile_dir} ${version} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType} ${pagMode}
else
    cd ${script_dir}/tools
    ./makeclient.sh ${compile_dir} ${version} "${build_time}" ${cpuType} ${osType} ${verMode} ${verType}
fi

# 4. Clean up temporary compile directories
#${csudo} rm -rf ${compile_dir}
 
