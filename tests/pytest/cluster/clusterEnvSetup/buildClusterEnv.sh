#!/bin/bash
echo "Executing buildClusterEnv.sh"
DOCKER_DIR=/data
CURR_DIR=`pwd`

if [ $# != 4 ]; then 
  echo "argument list need input : "
  echo "  -n numOfNodes"
  echo "  -v version"  
  exit 1
fi

NUM_OF_NODES=
VERSION=
while getopts "n:v:" arg
do
  case $arg in
    n)
      NUM_OF_NODES=$OPTARG
      ;;
    v)
      VERSION=$OPTARG
      ;;    
    ?)
      echo "unkonwn argument"
      ;;
  esac
done


function createDIR {
  for i in {1.. $2}
  do    
    mkdir -p /data/node$i/data
    mkdir -p /data/node$i/log
    mkdir -p /data/node$i/cfg
    mkdir -p /data/node$i/core
  done
}

function cleanEnv {    
  for i in {1..3}
  do
    echo /data/node$i/data/*
    rm -rf /data/node$i/data/*
    echo /data/node$i/log/*
    rm -rf /data/node$i/log/*
  done
}

function prepareBuild {

  if [ -d $CURR_DIR/../../../../release ]; then
    echo release exists
    rm -rf $CURR_DIR/../../../../release/*
  fi


  if [ ! -e $DOCKER_DIR/TDengine-server-$VERSION-Linux-x64.tar.gz && ! -e TDengine-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
    cd $CURR_DIR/../../../../packaging
    ./release.sh -v edge -n $VERSION >> /dev/null

    if [ ! -e $CURR_DIR/../../../../release/TDengine-server-$VERSION-Linux-x64.tar.gz ]; then
      echo "no TDengine install package found"
      exit 1
    fi

    if [ ! -e $CURR_DIR/../../../../release/TDengine-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
      echo "no arbitrator install package found"
      exit 1
    fi

    cd $CURR_DIR/../../../../release
    mv TDengine-server-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
    mv TDengine-arbitrator-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
  fi
  
  rm -rf $DOCKER_DIR/*.yml
  cd $CURR_DIR

  cp docker-compose.yml  $DOCKER_DIR
  cp Dockerfile $DOCKER_DIR

  if [ $NUM_OF_NODES -eq 4 ]; then
    cp ../node4.yml $DOCKER_DIR
  fi

  if [ $NUM_OF_NODES -eq 5 ]; then
    cp ../node5.yml $DOCKER_DIR
  fi
}

function clusterUp {
  
  cd $DOCKER_DIR

  if [ $NUM_OF_NODES -eq 3 ]; then
    PACKAGE=TDengine-server-$VERSION-Linux-x64.tar.gz DIR=TDengine-server-$VERSION VERSION=$VERSION docker-compose up -d
  fi

  if [ $NUM_OF_NODES -eq 4 ]; then
    PACKAGE=TDengine-server-$VERSION-Linux-x64.tar.gz DIR=TDengine-server-$VERSION VERSION=$VERSION docker-compose -f docker-compose.yml -f node4.yml up -d
  fi

  if [ $NUM_OF_NODES -eq 5 ]; then
    PACKAGE=TDengine-server-$VERSION-Linux-x64.tar.gz DIR=TDengine-server-$VERSION VERSION=$VERSION docker-compose -f docker-compose.yml -f node4.yml -f node5.yml up -d
  fi
}

cleanEnv 
prepareBuild
clusterUp