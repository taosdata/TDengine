#!/bin/bash
#
# create docker-compose.yml

set -e
#set -x

# set parameters by default value
composeYmlFile="./docker-compose.yml"
dnodeNumber=1
subnet="172.33.0.0/16"

while getopts "hn:f:s:" arg
do
  case $arg in
    n)
      dnodeNumber=$(echo $OPTARG)
      ;;
    f)
      composeYmlFile=$(echo $OPTARG)
      ;;
    s)
      subnet=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -n [ dnode number] "
      echo "                     -f [ yml file] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "dnodeNumber=${dnodeNumber} composeYmlFile=${composeYmlFile}"

createFirstSection() {
  ymlFile=$1  
    
  echo "version: '3.7'"                                                 > ${ymlFile}
  echo ""                                                              >> ${ymlFile}
  echo "x-node: &x-node"                                               >> ${ymlFile}
  echo "  build:"                                                      >> ${ymlFile}
  echo "    context: ."                                                >> ${ymlFile}
  echo "    dockerfile: ./tdserver/Dockerfile"                         >> ${ymlFile}
  echo "    args:"                                                     >> ${ymlFile}
  echo "      - PACKAGE=TDengine-server-3.0.0.0-Linux-x64.tar.gz"      >> ${ymlFile}
  echo "      - EXTRACTDIR=TDengine-server-3.0.0.0"                    >> ${ymlFile}
  echo "  image: 'tdengine:3.0.0.0'"                                   >> ${ymlFile}
  echo "  container_name: 'node1'"                                     >> ${ymlFile}
  echo "  privileged: true"                                            >> ${ymlFile}
  echo "  cap_add:"                                                    >> ${ymlFile}
  echo "    - ALL"                                                     >> ${ymlFile}
  echo "  stdin_open: true"                                            >> ${ymlFile}
  echo "  tty: true"                                                   >> ${ymlFile}
  echo "  environment:"                                                >> ${ymlFile}
  echo '    TZ: "Asia/Shanghai"'                                       >> ${ymlFile}
  echo '  command: >'                                                  >> ${ymlFile}
  echo '    sh -c "ln -snf /usr/share/zoneinfo/$TZ /etc/localtime &&'  >> ${ymlFile}
  echo '    echo $TZ > /etc/timezone &&'                               >> ${ymlFile}
  echo '    exec sysctl -w kernel.core_pattern=/corefile/core-%e-%p"'  >> ${ymlFile}
  echo "  restart: always"                                             >> ${ymlFile}
  echo "  hostname: node1"                                             >> ${ymlFile}
  echo "  command: taosd"                                              >> ${ymlFile}
  echo "  deploy:"                                                     >> ${ymlFile}
  echo "    resources:"                                                >> ${ymlFile}
  echo "       limits:"                                                >> ${ymlFile}
  echo '          cpus: "2.00"'                                        >> ${ymlFile}
  echo "          memory: 4G"                                          >> ${ymlFile}
  echo "       reservations:"                                          >> ${ymlFile}
  echo '          cpus: "1.00"'                                        >> ${ymlFile}
  echo "          memory: 500M"                                        >> ${ymlFile}
  echo "  volumes:"                                                    >> ${ymlFile}
  echo "    - /etc/localtime:/etc/localtime:ro"                        >> ${ymlFile}
  echo '    - $PWD:/work'                                              >> ${ymlFile}
  echo '    - $PWD/storage/dnode1/data:/var/lib/taos'                  >> ${ymlFile}
  echo '    - $PWD/storage/dnode1/log:/var/log/taos'                   >> ${ymlFile}
  echo '    - $PWD/storage/dnode1/cfg:/etc/taos'                       >> ${ymlFile}
  echo '    - $PWD/storage/dnode1/core:/corefile'                      >> ${ymlFile}
  echo ""                                                              >> ${ymlFile}
  echo "networks:"                                                     >> ${ymlFile}
  echo "  tdnet:"                                                      >> ${ymlFile}
  echo "    ipam:"                                                     >> ${ymlFile}
  echo "      driver: default"                                         >> ${ymlFile}  
  echo "      config:"                                                 >> ${ymlFile}
  echo "        - subnet: ${subnet}"                                   >> ${ymlFile}  
  echo ""                                                              >> ${ymlFile}
  echo "services:"                                                     >> ${ymlFile}
}

createSingleDnodesCfg() {
  ymlFile=$1
  index=$2
  
  ipPrefix=${subnet%.*}
  
  let ipIndex=index+1

  echo "  node${index}:"                                         >> ${ymlFile}
  echo "    <<: *x-node"                                         >> ${ymlFile}
  echo "    container_name: 'node${index}'"                      >> ${ymlFile}
  echo "    hostname: node${index}"                              >> ${ymlFile}
  echo "    networks:"                                           >> ${ymlFile}
  echo "      tdnet:"                                            >> ${ymlFile}
  echo "        ipv4_address: ${ipPrefix}.${ipIndex}"            >> ${ymlFile}
  echo "    volumes:"                                            >> ${ymlFile}
  echo "      - /etc/localtime:/etc/localtime:ro"                >> ${ymlFile}
  echo "      - \$PWD:/work"                                     >> ${ymlFile}
  echo "      - \$PWD/storage/dnode${index}/data:/var/lib/taos"  >> ${ymlFile}
  echo "      - \$PWD/storage/dnode${index}/log:/var/log/taos"   >> ${ymlFile}
  echo "      - \$PWD/storage/dnode${index}/cfg:/etc/taos"       >> ${ymlFile}
  echo "      - \$PWD/storage/dnode${index}/core:/corefile"      >> ${ymlFile}
  echo ""                                                        >> ${ymlFile}
}

createDnodesOfDockerComposeYml() {
  ymlFile=$1
  dnodeNumber=$2
  
  for ((i=1; i<=${dnodeNumber}; i++)); do
    createSingleDnodesCfg ${ymlFile} ${i}
  done
}

########################################################################################
###############################  main process ##########################################

createFirstSection ${composeYmlFile}
createDnodesOfDockerComposeYml ${composeYmlFile} ${dnodeNumber} 

echo "====create docker-compose.yml end===="
echo " "


