#!/bin/sh
#parameter
scriptDir=$(dirname $(readlink -f $0))
packgeName=$1
version=$2
originPackageName=$3
originversion=$4
testFile=$5
subFile="taos.tar.gz"
password=$6

# Color setting
RED='\033[41;30m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
GREEN_DARK='\033[0;32m'
YELLOW_DARK='\033[0;33m'
BLUE_DARK='\033[0;34m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

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
command=$1
if command -v ${command} ;then
    echoColor YD  "${command} is already installed" 
else 
    if command -v apt ;then
        apt-get install ${command} -y 
    elif command -v yum ;then
        yum -y install ${command} 
        echoColor YD "you should install ${command} manually"
    fi
fi
}

function echoColor {
color=$1    
command=$2

if [ ${color} = 'Y' ];then
    echo -e  "${YELLOW}${command}${NC}" 
elif [ ${color} = 'YD' ];then
    echo -e  "${YELLOW_DARK}${command}${NC}" 
elif [ ${color} = 'R' ];then
    echo -e  "${RED}${command}${NC}" 
elif [ ${color} = 'G' ];then
    echo  -e  "${GREEN}${command}${NC}\r\n" 
elif [ ${color} = 'B' ];then
    echo  -e  "${BLUE}${command}${NC}" 
elif [ ${color} = 'BD' ];then
    echo  -e  "${BLUE_DARK}${command}${NC}" 
fi
}


echoColor G "===== install basesoft ====="

cmdInstall tree
cmdInstall wget
cmdInstall sshpass

echoColor G "===== Uninstall all components of TDeingne ====="

if command -v rmtaos ;then
    echoColor YD "uninstall all components of TDeingne:rmtaos"
    rmtaos 
else 
     echoColor YD "os doesn't include TDengine"
fi

if command -v rmtaostools ;then
    echoColor YD "uninstall all components of TDeingne:rmtaostools"
    rmtaostools
else 
    echoColor YD "os doesn't include rmtaostools "
fi




echoColor G "===== new workroom path ====="
installPath="/usr/local/src/packageTest"
oriInstallPath="/usr/local/src/packageTest/3.1"

if [ ! -d ${installPath} ] ;then
    echoColor BD "mkdir -p ${installPath}"
    mkdir -p ${installPath}
else  
    echoColor YD "${installPath} already exists"
fi

if [ -d ${installPath}/${tdPath} ] ;then
    echoColor BD "rm -rf ${installPath}/${tdPath}/*"
    rm -rf ${installPath}/${tdPath}/*
fi

if [ ! -d ${oriInstallPath} ] ;then
    echoColor BD "mkdir -p ${oriInstallPath}"
    mkdir -p ${oriInstallPath}
else  
    echoColor YD "${oriInstallPath} already exists"
fi

if [ -d ${oriInstallPath}/${originTdpPath} ] ;then
    echoColor BD "rm -rf ${oriInstallPath}/${originTdpPath}/*"
    rm -rf  ${oriInstallPath}/${originTdpPath}/*  
fi


echoColor G "===== download  installPackage ====="
# cd ${installPath}
# wget https://www.taosdata.com/assets-download/3.0/${packgeName}
# cd  ${oriInstallPath}
# wget https://www.taosdata.com/assets-download/3.0/${originPackageName}

cd ${installPath}
cp -r ${scriptDir}/debRpmAutoInstall.sh   . 

if [ ! -f  {packgeName}  ];then
    echoColor  BD "sshpass -p ${password} scp -oStrictHostKeyChecking=no 192.168.1.131:/nas/TDengine3/v${version}/community/${packgeName}  ."
    sshpass -p ${password} scp -oStrictHostKeyChecking=no -oStrictHostKeyChecking=no 192.168.1.131:/nas/TDengine3/v${version}/community/${packgeName}  .
fi

packageSuffix=$(echo ${packgeName}  | awk -F '.' '{print $NF}')


if [ ! -f  debRpmAutoInstall.sh  ];then
    echo '#!/usr/bin/expect ' >  debRpmAutoInstall.sh
    echo 'set packgeName [lindex $argv 0]' >>  debRpmAutoInstall.sh
    echo 'set packageSuffix [lindex $argv 1]' >>  debRpmAutoInstall.sh
    echo 'set timeout 3 ' >>  debRpmAutoInstall.sh
    echo 'if { ${packageSuffix} == "deb" } {' >>  debRpmAutoInstall.sh
    echo '    spawn  dpkg -i ${packgeName} '  >>  debRpmAutoInstall.sh
    echo '} elseif { ${packageSuffix} == "rpm"} {' >>  debRpmAutoInstall.sh
    echo '    spawn rpm -ivh ${packgeName}'  >>  debRpmAutoInstall.sh
    echo '}' >>  debRpmAutoInstall.sh
    echo 'expect "*one:"' >>  debRpmAutoInstall.sh
    echo 'send  "\r"' >>  debRpmAutoInstall.sh
    echo 'expect "*skip:"' >>  debRpmAutoInstall.sh
    echo 'send  "\r" ' >>  debRpmAutoInstall.sh
fi


echoColor G "===== instal Package ====="

if [[ ${packgeName} =~ "deb" ]];then
    cd ${installPath}
    dpkg -r taostools
    dpkg -r tdengine
    if [[ ${packgeName} =~ "TDengine" ]];then
        echoColor BD "./debRpmAutoInstall.sh ${packgeName}  ${packageSuffix}" &&   chmod 755 debRpmAutoInstall.sh &&  ./debRpmAutoInstall.sh  ${packgeName}  ${packageSuffix}
    else
        echoColor BD "dpkg  -i ${packgeName}" &&   dpkg  -i ${packgeName}
    fi
elif [[ ${packgeName} =~ "rpm" ]];then
    cd ${installPath}
    sudo rpm -e tdengine
    sudo rpm -e taostools
    if [[ ${packgeName} =~ "TDengine" ]];then
        echoColor BD "./debRpmAutoInstall.sh ${packgeName}  ${packageSuffix}" &&   chmod 755 debRpmAutoInstall.sh &&  ./debRpmAutoInstall.sh  ${packgeName}  ${packageSuffix}
    else
        echoColor BD "rpm  -ivh ${packgeName}" &&   rpm  -ivh ${packgeName}
    fi
elif [[ ${packgeName} =~ "tar" ]];then
    echoColor G "===== check installPackage File of tar ====="
    cd  ${oriInstallPath}
    if [ ! -f  {originPackageName}  ];then
        echoColor YD "download  base installPackage"
        echoColor BD "sshpass -p ${password} scp -oStrictHostKeyChecking=no 192.168.1.131:/nas/TDengine3/v${originversion}/community/${originPackageName} ."
        sshpass -p ${password} scp -oStrictHostKeyChecking=no 192.168.1.131:/nas/TDengine3/v${originversion}/community/${originPackageName} .
    fi
    echoColor YD "unzip the base installation package" 
    echoColor BD "tar -xf ${originPackageName}" && tar -xf ${originPackageName} 
    cd ${installPath} 
    echoColor YD "unzip the new installation package" 
    echoColor BD "tar -xf ${packgeName}" && tar -xf ${packgeName} 

    if [ ${testFile} != "tools" ] ;then
        cd ${installPath}/${tdPath} && tar xf ${subFile}
        cd  ${oriInstallPath}/${originTdpPath}  && tar xf ${subFile}
    fi

    cd  ${oriInstallPath}/${originTdpPath} && tree  >  ${installPath}/base_${originversion}_checkfile
    cd ${installPath}/${tdPath}   && tree > ${installPath}/now_${version}_checkfile
    
    cd ${installPath} 
    diff  ${installPath}/base_${originversion}_checkfile   ${installPath}/now_${version}_checkfile  > ${installPath}/diffFile.log
    diffNumbers=`cat ${installPath}/diffFile.log |wc -l `

    if [ ${diffNumbers} != 0 ];then
        echoColor R "The number and names of files is different from the previous installation package"
        echoColor Y `cat ${installPath}/diffFile.log`
        exit -1
    else 
        echoColor G "The number and names of files are the same as previous installation packages"
    fi
    echoColor YD  "===== install Package of tar ====="
    cd ${installPath}/${tdPath}
    if [ ${testFile} = "server" ];then
        echoColor BD "bash ${installCmd}  -e no  "
        bash ${installCmd}  -e no  
    else
        echoColor BD "bash ${installCmd} "
        bash ${installCmd} 
    fi
fi  

cd ${installPath}

if ([[ ${packgeName} =~ "Lite" ]] &&  [[ ${packgeName} =~ "tar" ]]) ||   [[ ${packgeName} =~ "client" ]] ;then
    echoColor G "===== install taos-tools when package is lite or client ====="
    cd ${installPath}
    sshpass -p ${password}   scp -oStrictHostKeyChecking=no 192.168.1.131:/nas/TDengine3/v${version}/community/taosTools-2.1.2-Linux-x64.tar.gz .
    # wget https://www.taosdata.com/assets-download/3.0/taosTools-2.1.2-Linux-x64.tar.gz
    tar xf taosTools-2.1.2-Linux-x64.tar.gz
    cd taosTools-2.1.2 && bash install-taostools.sh
elif [[ ${packgeName} =~ "Lite" ]] &&  [[ ${packgeName} =~ "deb" ]] ;then
    echoColor G "===== install taos-tools when package is lite or client ====="
    cd ${installPath}
    sshpass -p ${password}   scp -oStrictHostKeyChecking=no 192.168.1.131:/nas/TDengine3/v${version}/community/taosTools-2.1.2-Linux-x64.tar.gz .
    tar xf taosTools-2.1.2-Linux-x64.tar.gz
    cd taosTools-2.1.2 && bash install-taostools.sh
elif [[ ${packgeName} =~ "Lite" ]] &&  [[ ${packgeName} =~ "rpm" ]]  ;then
    echoColor G "===== install taos-tools when package is lite or client ====="
    cd ${installPath}
    sshpass -p ${password}   scp -oStrictHostKeyChecking=no -oStrictHostKeyChecking=no 192.168.1.131:/nas/TDengine3/v${version}/community/taosTools-2.1.2-Linux-x64.tar.gz .
    tar xf taosTools-2.1.2-Linux-x64.tar.gz
    cd taosTools-2.1.2 && bash install-taostools.sh
fi

