#!/bin/bash
echo "Executing cleanClusterEnv.sh"
CURR_DIR=`pwd`

if [ $# != 2 ]; then 
  echo "argument list need input : "  
  echo "  -d docker dir" 
  exit 1
fi

DOCKER_DIR=
while getopts "d:" arg
do
  case $arg in
    d)
      DOCKER_DIR=$OPTARG
      ;;    
    ?)
      echo "unkonwn argument"
      ;;
  esac
done

function removeDockerContainers {
    cd $DOCKER_DIR
    docker-compose down --remove-orphans
}

function cleanEnv {
  echo "Clean up docker environment"    
  for i in {1..5}
  do    
    rm -rf $DOCKER_DIR/node$i/data/*    
    rm -rf $DOCKER_DIR/node$i/log/*
  done
}

removeDockerContainers
cleanEnv