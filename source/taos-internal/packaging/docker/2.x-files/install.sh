#!/bin/bash

# ********************************************************
# Script to install docker-version TAOSDATA on computer
# ********************************************************

set -e

# Global variables
tarFile="tdengine.tar.gz"

headerDir="/usr/local/include/tdengine"
cfgDir="/etc/tdengine"
binDir="/usr/local/bin/tdengine"
libDir="/usr/local/lib/tdengine"
linkDir="/usr/bin"

javaAppDir="connector"


# TODO: Function to install different parts.
make_directory() {
    sudo mkdir -p ${cfgDir} ${headerDir} ${binDir} ${libDir} ${binDir}/connector
    # Copy global configure file
    sudo cp -n cfg/tdengine.cfg ${cfgDir} 
}

installTDengine() {
    # TODO: check if program is installed
    make_directory
    # Build tdengined image
    sudo docker container rm -f tdengined_img || true
    sudo docker image rm tdengined_img || true
    sudo docker build -t tdengined_img -f Dockerfile.tdengined .
    # Deploy the service
    sudo docker run -d --name tdengined --network="host" \
        --mount source=taos_data,target=/var/lib/tdengine/ \
        --mount source=taos_log,target=/var/log/tdengine/ \
        --mount type=bind,source=/etc/tdengine/,target=/etc/tdengine/ \
        --restart=always \
        tdengined_img
}


installOthers() {
    # Update header file
    sudo rm -f ${headerDir}/*.h && sudo cp inc/*.h ${headerDir}

    # Update lib file
    sudo rm -f /usr/lib/libtaos.so /usr/lib/libtaos.a
    sudo rm -f ${libDir}/* && sudo cp lib/* ${libDir}
    sudo ln -s ${libDir}/libtaos.so /usr/lib/libtaos.so
    sudo ln -s ${libDir}/libtaos.a /usr/lib/libtaos.a

    # Update JDBC
    sudo rm -rf ${binDir}/connector/*
    sudo cp JDBCDriver*-dist.* ${binDir}/connector 2> /dev/null || :

    # TODO: Install taos
    sudo rm -f ${linkDir}/taos ${binDir}/taos.sh
    sudo docker image rm taos_img || true
    sudo docker build --no-cache -t taos_img -f Dockerfile.tdengine .
    sudo echo '#!/bin/bash'                                                > taos.sh
    sudo echo                                                             >> taos.sh
    sudo echo 'docker run -it --rm --network="host" \'                    >> taos.sh
    sudo echo '--mount type=bind,source=/etc/tdengine/,target=/etc/tdengine/ \' >> taos.sh
    sudo echo '--mount type=bind,source="$HOME",target=/root \'           >> taos.sh
    sudo echo 'taos_img $@'                                               >> taos.sh
    sudo mv taos.sh ${binDir}
    sudo chmod a+x ${binDir}/taos.sh
    sudo ln -s ${binDir}/taos.sh ${linkDir}/taos

    # Install remove.sh
    sudo rm -f ${linkDir}/rmtaos ${binDir}/remove.sh
    sudo cp bin/remove.sh ${binDir}
    sudo chmod a+x ${binDir}/remove.sh
    sudo ln -s ${binDir}/remove.sh ${linkDir}/rmtaos
}

printInstallGuide() {
    echo
    echo "Type 'bash install.sh' to install management and data service"
    echo "Type 'bash install.sh dnode' to install data service only"
    echo "Type 'bash install.sh mgmt' to install management service only"
}

# ----------------------- Main program -----------------------
tar -zxf ${tarFile}

installTDengine
installOthers

rm -rf $(tar -tf ${tarFile})
