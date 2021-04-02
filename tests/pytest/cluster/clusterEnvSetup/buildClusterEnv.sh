#!/bin/bash
echo "Executing buildClusterEnv.sh"
CURR_DIR=`pwd`
IN_TDINTERNAL="community"

if [ $# != 6 ]; then 
  echo "argument list need input : "
  echo "  -n numOfNodes"
  echo "  -v version"
  echo "  -d docker dir" 
  exit 1
fi

NUM_OF_NODES=
VERSION=
DOCKER_DIR=
while getopts "n:v:d:" arg
do
  case $arg in
    n)
      NUM_OF_NODES=$OPTARG
      ;;
    v)
      VERSION=$OPTARG
      ;;
    d)
      DOCKER_DIR=$OPTARG
      ;;    
    ?)
      echo "unkonwn argument"
      ;;
  esac
done

function addTaoscfg {
  for((i=1;i<=$NUM_OF_NODES;i++))
  do 
    touch $DOCKER_DIR/node$i/cfg/taos.cfg
    echo 'firstEp          tdnode1:6030' > $DOCKER_DIR/node$i/cfg/taos.cfg
    echo 'fqdn             tdnode'$i >> $DOCKER_DIR/node$i/cfg/taos.cfg
    echo 'arbitrator       tdnode1:6042' >> $DOCKER_DIR/node$i/cfg/taos.cfg
  done
}

function createDIR {
  for((i=1;i<=$NUM_OF_NODES;i++))
  do    
    mkdir -p $DOCKER_DIR/node$i/data
    mkdir -p $DOCKER_DIR/node$i/log
    mkdir -p $DOCKER_DIR/node$i/cfg
    mkdir -p $DOCKER_DIR/node$i/core
  done
}

function cleanEnv {
  echo "Clean up docker environment"    
  for((i=1;i<=$NUM_OF_NODES;i++))
  do    
    rm -rf $DOCKER_DIR/node$i/data/*    
    rm -rf $DOCKER_DIR/node$i/log/*
  done
}

function prepareBuild {

  if [ -d $CURR_DIR/../../../../release ]; then
    echo release exists
    rm -rf $CURR_DIR/../../../../release/*
  fi

  cd $CURR_DIR/../../../../packaging 

  if [[ "$CURR_DIR" == *"$IN_TDINTERNAL"* ]]; then
    if [ ! -e $DOCKER_DIR/TDengine-enterprise-server-$VERSION-Linux-x64.tar.gz ] || [ ! -e $DOCKER_DIR/TDengine-enterprise-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
              
      echo "generating TDeninge enterprise packages"
      ./release.sh -v cluster -n $VERSION >> /dev/null 2>&1
      
      if [ ! -e $CURR_DIR/../../../../release/TDengine-enterprise-server-$VERSION-Linux-x64.tar.gz ]; then
        echo "no TDengine install package found"
        exit 1
      fi

      if [ ! -e $CURR_DIR/../../../../release/TDengine-enterprise-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
        echo "no arbitrator install package found"
        exit 1
      fi

      cd $CURR_DIR/../../../../release
      mv TDengine-enterprise-server-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
      mv TDengine-enterprise-arbitrator-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
    fi
  else
    if [ ! -e $DOCKER_DIR/TDengine-server-$VERSION-Linux-x64.tar.gz ] || [ ! -e $DOCKER_DIR/TDengine-arbitrator-$VERSION-Linux-x64.tar.gz ]; then

      echo "generating TDeninge community packages"
      ./release.sh -v edge -n $VERSION >> /dev/null 2>&1
      
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
  fi
  
  rm -rf $DOCKER_DIR/*.yml
  cd $CURR_DIR

  cp *.yml  $DOCKER_DIR
  cp Dockerfile $DOCKER_DIR
}

function clusterUp {
  echo "docker compose start"
  
  cd $DOCKER_DIR  

  if [[ "$CURR_DIR" == *"$IN_TDINTERNAL"* ]]; then
    docker_run="PACKAGE=TDengine-enterprise-server-$VERSION-Linux-x64.tar.gz TARBITRATORPKG=TDengine-enterprise-arbitrator-$VERSION-Linux-x64.tar.gz DIR=TDengine-enterprise-server-$VERSION DIR2=TDengine-enterprise-arbitrator-$VERSION VERSION=$VERSION DATADIR=$DOCKER_DIR docker-compose -f docker-compose.yml "
  else
    docker_run="PACKAGE=TDengine-server-$VERSION-Linux-x64.tar.gz TARBITRATORPKG=TDengine-arbitrator-$VERSION-Linux-x64.tar.gz DIR=TDengine-server-$VERSION DIR2=TDengine-arbitrator-$VERSION VERSION=$VERSION DATADIR=$DOCKER_DIR docker-compose -f docker-compose.yml "
  fi

  if [ $NUM_OF_NODES -ge 2 ];then
    echo "create $NUM_OF_NODES dnodes"
    for((i=3;i<=$NUM_OF_NODES;i++))
    do
      if [ ! -f node$i.yml ];then
        echo "node$i.yml not exist"
        cp node3.yml node$i.yml
        sed -i "s/td2.0-node3/td2.0-node$i/g" node$i.yml
        sed -i "s/'tdnode3'/'tdnode$i'/g" node$i.yml
        sed -i "s#/node3/#/node$i/#g" node$i.yml
        sed -i "s#hostname: tdnode3#hostname: tdnode$i#g" node$i.yml
        sed -i "s#ipv4_address: 172.27.0.9#ipv4_address: 172.27.0.`expr $i + 6`#g" node$i.yml
      fi
      docker_run=$docker_run" -f node$i.yml "
    done
    docker_run=$docker_run" up -d"
  fi
  echo $docker_run |sh 
  
  echo "docker compose finish"
}

createDIR
cleanEnv
addTaoscfg
prepareBuild
clusterUp