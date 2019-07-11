#!/bin/bash
#
# Generate the deb package for ubunt, or rpm package for centos, or tar.gz package for other linux os

set -e
#set -x

curr_dir=$(pwd)
script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -m ${script_dir}/..)"
versioninfo="${top_dir}/src/util/src/version.c"

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
echo "char version[64] = \"${version}\";" > ${versioninfo}
echo "char compatible_version[64] = \"${compatible_version}\";" >> ${versioninfo}
echo "char gitinfo[128] = \"$(git rev-parse --verify HEAD)\";"  >> ${versioninfo}
echo "char buildinfo[512] = \"Built by ${USER} at ${build_time}\";"  >> ${versioninfo}

# 2. cmake executable file
#default use debug mode
compile_mode="debug"
if [[ $1 == "Release" ]] || [[ $1 == "release" ]]; then
  compile_mode="Release"
fi

compile_dir="${top_dir}/${compile_mode}"
if [ -d ${compile_dir} ]; then
	 rm -rf ${compile_dir}
fi

mkdir -p ${compile_dir}
cd ${compile_dir}
cmake -DCMAKE_BUILD_TYPE=${compile_mode} ${top_dir}
make

cd ${curr_dir}

# 3. judge the operating system type, then Call the corresponding script for packaging
osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
#osinfo=$(cat /etc/os-release | grep "NAME" | cut -d '"' -f2)
#echo "osinfo: ${osinfo}"

if echo $osinfo | grep -qwi "ubuntu" ; then
  echo "this is ubuntu system"
  output_dir="${top_dir}/debs"
  if [ -d ${output_dir} ]; then
	 rm -rf ${output_dir}
  fi  
  mkdir -p ${output_dir} 
  cd ${script_dir}/deb
  ./makedeb.sh ${compile_dir} ${output_dir} ${version}
  
elif  echo $osinfo | grep -qwi "centos" ; then
  echo "this is centos system"
  output_dir="${top_dir}/rpms"
  if [ -d ${output_dir} ]; then
	 rm -rf ${output_dir}
  fi
  mkdir -p ${output_dir}  
  cd ${script_dir}/rpm
  ./makerpm.sh ${compile_dir} ${output_dir} ${version}
  
else
  echo "this is other linux system"  
fi

cd ${script_dir}/tools
./makepkg.sh ${compile_dir} ${version} "${build_time}" 

# 4. Clean up temporary compile directories
#rm -rf ${compile_dir}
 
