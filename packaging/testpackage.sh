#!/bin/sh


packgeName=$1
version=$2
originPackageName=$3
originversion=$4
testFile=$5
subFile="taos.tar.gz"
password=$6

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
cmdInstall sshpass

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




echo "download  installPackage"
# cd ${installPath}
# wget https://www.taosdata.com/assets-download/3.0/${packgeName}
# cd  ${oriInstallPath}
# wget https://www.taosdata.com/assets-download/3.0/${originPackageName}

cd ${installPath}
if [ ! -f  {packgeName}  ];then
    sshpass -p ${password} scp 192.168.1.131:/nas/TDengine3/v${version}/community/${packgeName}  .
fi
if [ ! -f  debAuto.sh  ];then
    echo '#!/usr/bin/expect ' >  debAuto.sh
    echo 'set timeout 3 ' >>  debAuto.sh
    echo 'pset packgeName [lindex $argv 0]' >>  debAuto.sh
    echo 'spawn dpkg -i ${packgeName}' >>  debAuto.sh
    echo 'expect "*one:"' >>  debAuto.sh
    echo 'send  "\r"' >>  debAuto.sh
    echo 'expect "*skip:"' >>  debAuto.sh
    echo 'send  "\r" ' >>  debAuto.sh
fi

if [[ ${packgeName} =~ "deb" ]];then
    cd ${installPath}
    dpkg -r taostools
    dpkg -r tdengine
    if [[ ${packgeName} =~ "TDengine" ]];then
        echo "./debAuto.sh ${packgeName}" &&   chmod 755 debAuto.sh &&  ./debAuto.sh ${packgeName}
    else
        echo "dpkg  -i ${packgeName}" &&   dpkg  -i ${packgeName}
    fi
elif [[ ${packgeName} =~ "rpm" ]];then
    cd ${installPath}
    echo "rpm ${packgeName}"  && rpm -ivh ${packgeName}  --quiet 
elif [[ ${packgeName} =~ "tar" ]];then
    cd  ${oriInstallPath}
    if [ ! -f  {originPackageName}  ];then
        sshpass -p ${password} scp 192.168.1.131:/nas/TDengine3/v${originversion}/community${originPackageName} .
    fi
    echo "tar -xvf ${originPackageName}" && tar -xvf ${originPackageName} 

    cd ${installPath} 
    echo "tar -xvf ${packgeName}" && tar -xvf ${packgeName} 


    if [ ${testFile} != "tools" ] ;then
        cd ${installPath}/${tdPath} && tar vxf ${subFile}
        cd  ${oriInstallPath}/${originTdpPath}  && tar vxf ${subFile}
    fi

    echo "check installPackage File"

    cd ${installPath} 

    tree ${oriInstallPath}/${originTdpPath} >  ${oriInstallPath}/${originPackageName}_checkfile
    tree ${installPath}/${tdPath} > ${installPath}/${packgeName}_checkfile

    diff  ${installPath}/${packgeName}_checkfile ${oriInstallPath}/${originPackageName}_checkfile  > ${installPath}/diffFile.log
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
    if [[ ${packgeName} =~ "Lite" ]] &&  [[ ${packgeName} =~ "tar" ]]  ;then
        cd ${installPath}
        sshpass -p ${password}   scp 192.168.1.131:/nas/TDengine3/v${version}/community/taosTools-2.1.2-Linux-x64.tar.gz .
        # wget https://www.taosdata.com/assets-download/3.0/taosTools-2.1.2-Linux-x64.tar.gz
        tar xvf taosTools-2.1.2-Linux-x64.tar.gz
        cd taosTools-2.1.2 && bash install-taostools.sh
    elif [[ ${packgeName} =~ "Lite" ]] &&  [[ ${packgeName} =~ "deb" ]] ;then
        cd ${installPath}
        sshpass -p ${password}   scp 192.168.1.131:/nas/TDengine3/v${version}/community/taosTools-2.1.2-Linux-x64.deb .
        dpkg -i taosTools-2.1.2-Linux-x64.deb 
    elif [[ ${packgeName} =~ "Lite" ]] &&  [[ ${packgeName} =~ "rpm" ]]  ;then
        cd ${installPath}
        sshpass -p ${password}   scp 192.168.1.131:/nas/TDengine3/v${version}/community/taosTools-2.1.2-Linux-x64.rpm .
        rpm -ivh taosTools-2.1.2-Linux-x64.rpm --quiet 
    fi

fi  

