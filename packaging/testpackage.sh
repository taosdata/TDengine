#!/bin/sh


packgeName=$1
version=$2
originPackageName=$3
originversion=$4
testFile=$5
subFile="taos.tar.gz"

if [ ${testFile} = "server" ];then
    tdPath="TDengine-server-${version}"
    originTdpPath="TDengine-server-${originversion}"
    installCmd="install.sh"
elif [ ${testFile} = "client" ];then
    tdPath="TDengine-client-${version}"
    originTdpPath="TDengine-client-${originversion}"
    installCmd="install_client.sh"    
elif [ ${testFile} = "tools" ];then
    tdPath="taosTools-${version}"
    originTdpPath="taosTools-${originversion}"
    installCmd="install-taostools.sh"
fi

function cmdInstall {
comd=$1
if command -v ${comd} ;then
    echo "${comd} is already installed" 
else 
    if command -v apt ;then
        apt-get install ${comd} -y 
    elif command -v yum ;then
        yum -y install ${comd} 
        echo "you should install ${comd} manually"
    fi
fi
}


echo "Uninstall all components of TDeingne"

if command -v rmtaos ;then
    echo "uninstall all components of TDeingne:rmtaos"
    rmtaos 
else 
    echo "os doesn't include TDengine "
fi

if command -v rmtaostools ;then
    echo "uninstall all components of TDeingne:rmtaostools"
    rmtaostools
else 
    echo "os doesn't include rmtaostools "
fi


cmdInstall tree
cmdInstall wget

echo "new workroom path"
installPath="/usr/local/src/packageTest"
oriInstallPath="/usr/local/src/packageTest/3.1"

if [ ! -d ${installPath} ] ;then
    mkdir -p ${installPath}
else  
    echo "${installPath} already exists"
fi


if [ ! -d ${oriInstallPath} ] ;then
    mkdir -p ${oriInstallPath}
else  
    echo "${oriInstallPath} already exists"
fi

echo "decompress installPackage"

cd ${installPath}
wget https://www.taosdata.com/assets-download/3.0/${packgeName}
cd  ${oriInstallPath}
wget https://www.taosdata.com/assets-download/3.0/${originPackageName}


if [[ ${packgeName} =~ "deb" ]];then
    cd ${installPath}
    echo "dpkg ${packgeName}" &&  dpkg -i ${packgeName}
elif [[ ${packgeName} =~ "rpm" ]];then
    cd ${installPath}
    echo "rpm ${packgeName}"  && rpm -ivh ${packgeName}
elif [[ ${packgeName} =~ "tar" ]];then
    echo "tar ${packgeName}" && tar -xvf ${packgeName} 
    cd ${oriInstallPath}
    echo "tar -xvf ${originPackageName}" && tar -xvf ${originPackageName} 
    cd ${installPath} 
    echo "tar -xvf ${packgeName}" && tar -xvf ${packgeName} 


    if [ ${testFile} != "tools" ] ;then
        cd ${installPath}/${tdPath} && tar vxf ${subFile}
        cd  ${oriInstallPath}/${originTdpPath}  && tar vxf ${subFile}
    fi

    echo "check installPackage File"

    cd ${installPath} 

    tree ${oriInstallPath}/${originTdpPath} > ${originPackageName}_checkfile
    tree ${installPath}/${tdPath} > ${packgeName}_checkfile

    diff  ${packgeName}_checkfile  ${originPackageName}_checkfile  > ${installPath}/diffFile.log
    diffNumbers=`cat ${installPath}/diffFile.log |wc -l `
    if [ ${diffNumbers} != 0 ];then
        echo "The number and names of files have changed from the previous installation package"
        echo `cat ${installPath}/diffFile.log`
        exit -1
    fi

    cd ${installPath}/${tdPath}
    if [ ${testFile} = "server" ];then
        bash ${installCmd}  -e no  
    else
        bash ${installCmd} 
    fi
    if [[ ${packgeName} =~ "Lite" ]];then
        cd ${installPath}
        wget https://www.taosdata.com/assets-download/3.0/taosTools-2.1.2-Linux-x64.tar.gz
        tar xvf taosTools-2.1.2-Linux-x64.tar.gz
        cd taosTools-2.1.2 && bash install-taostools.sh
    fi

fi  

