#!/bin/sh

scriptDir=$(dirname $(readlink -f $0))

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
cp -r ${scriptDir}/debRpmAutoInstall.sh   . 

if [ ! -f  {packgeName}  ];then
    echo "sshpass -p ${password} scp 192.168.1.131:/nas/TDengine3/v${version}/community/${packgeName}  ."
    sshpass -p ${password} scp 192.168.1.131:/nas/TDengine3/v${version}/community/${packgeName}  .
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

if [[ ${packgeName} =~ "deb" ]];then
    cd ${installPath}
    dpkg -r taostools
    dpkg -r tdengine
    if [[ ${packgeName} =~ "TDengine" ]];then
        echo "./debRpmAutoInstall.sh ${packgeName}  ${packageSuffix}" &&   chmod 755 debRpmAutoInstall.sh &&  ./debRpmAutoInstall.sh  ${packgeName}  ${packageSuffix}
    else
        echo "dpkg  -i ${packgeName}" &&   dpkg  -i ${packgeName}
    fi
elif [[ ${packgeName} =~ "rpm" ]];then
    cd ${installPath}
    sudo rpm -e tdengine
    sudo rpm -e taostools
    if [[ ${packgeName} =~ "TDengine" ]];then
        echo "./debRpmAutoInstall.sh ${packgeName}  ${packageSuffix}" &&   chmod 755 debRpmAutoInstall.sh &&  ./debRpmAutoInstall.sh  ${packgeName}  ${packageSuffix}
    else
        echo "rpm  -ivh ${packgeName}" &&   rpm  -ivh ${packgeName}
    fi
elif [[ ${packgeName} =~ "tar" ]];then
    cd  ${oriInstallPath}
    if [ ! -f  {originPackageName}  ];then
        sshpass -p ${password} scp 192.168.1.131:/nas/TDengine3/v${originversion}/community/${originPackageName} .
    fi
    echo "tar -xvf ${originPackageName}" && tar -xvf ${originPackageName} 

    cd ${installPath} 
    echo "tar -xvf ${packgeName}" && tar -xvf ${packgeName} 


    if [ ${testFile} != "tools" ] ;then
        cd ${installPath}/${tdPath} && tar vxf ${subFile}
        cd  ${oriInstallPath}/${originTdpPath}  && tar vxf ${subFile}
    fi

    echo "check installPackage File"

    
    cd  ${oriInstallPath}/${originTdpPath} && tree  >  ${installPath}/base_${originversion}_checkfile
    cd ${installPath}/${tdPath}   && tree > ${installPath}/now_${version}_checkfile
    
    cd ${installPath} 
    diff  ${installPath}/base_${originversion}_checkfile   ${installPath}/now_${version}_checkfile  > ${installPath}/diffFile.log
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

