#!/bin/bash
#
# compile test version

set -e
#set -x

# compileVersion.sh  
#                -r [ TDengine project dir]  
#                -v [ TDengine branch version ]


projectDir=/root/TDengine
TDengineBrVer="3.0"

while getopts "hr:v:" arg
do
  case $arg in
    r)
      projectDir=$(echo $OPTARG)
      ;;
    v)
      TDengineBrVer=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -r [ TDengine project dir] "
      echo "                  -v [ TDengine branch version] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "projectDir=${projectDir} TDengineBrVer=${TDengineBrVer}" 

function gitPullBranchInfo () {
  branch_name=$1

  git checkout $branch_name
  echo "==== git pull $branch_name start ===="
##  git submodule update --init --recursive 
  git pull origin $branch_name ||:
  echo "==== git pull $branch_name end ===="
#  git pull --recurse-submodules
}

function compileTDengineVersion() {
    debugDir=debug
    if [ -d ${debugDir} ]; then
        rm -rf ${debugDir}/*    ||:
    else
        mkdir -p ${debugDir}
    fi
    
    cd ${debugDir}
    cmake ..
    make -j24   
    make install
}
########################################################################################
###############################  main process ##########################################

## checkout all branchs and git pull
cd ${projectDir}
gitPullBranchInfo $TDengineBrVer
compileTDengineVersion



