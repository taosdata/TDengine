#!/bin/sh


function usage() {
    echo "$0"
    echo -e "\t -f test file type,server/client/tools/"
    echo -e "\t -m pacakage version Type,community/enterprise"
    echo -e "\t -l package type,lite or not"
    echo -e "\t -c operation type,x64/arm64"
    echo -e "\t -v pacakage version,3.0.1.7"
    echo -e "\t -o pacakage version,3.0.1.7"
    echo -e "\t -s source Path,web/nas"
    echo -e "\t -t package Type,tar/rpm/deb"
    echo -e "\t -h help"
}


#parameter
scriptDir=$(dirname $(readlink -f $0))
version="3.0.1.7"
originversion="3.0.1.7"
testFile="server"
verMode="community"
sourcePath="nas"
cpuType="x64"
lite="true"
packageType="tar"
subFile="package.tar.gz"
while getopts "m:c:f:l:s:o:t:v:h" opt; do
    case $opt in
        m)
            verMode=$OPTARG
            ;;
        v)
            version=$OPTARG
            ;;
        f)
            testFile=$OPTARG
            ;;
        l)
            lite=$OPTARG
            ;;
        s)
            sourcePath=$OPTARG
            ;;
        o)
            originversion=$OPTARG
            ;;
        c)
            cpuType=$OPTARG
            ;;
        t)
            packageType=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        ?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 0
            ;;
    esac
done



echo "testFile:${testFile},verMode:${verMode},lite:${lite},cpuType:${cpuType},packageType:${packageType},version-${version},originversion:${originversion},sourcePath:${sourcePath}"
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

if [[ ${verMode} = "enterprise" ]];then
    if [ "${testFile}" == "server" ];then
        prePackage="TDengine-enterprise"
    elif [ "${testFile}" == "client" ];then
        prePackage="TDengine-enterprise-client"
    fi
elif [ ${verMode} = "community" ];then
    prePackage="TDengine-${testFile}"
fi
if [ ${lite} = "true" ];then
    packageLite="-Lite"
elif [ ${lite} = "false"  ];then
    packageLite=""
fi
if [[ "$packageType" = "tar" ]] ;then
    packageType="tar.gz"
fi

tdPath="${prePackage}-${version}"
originTdpPath="${prePackage}-${originversion}"

packageName="${tdPath}-Linux-${cpuType}${packageLite}.${packageType}"
originPackageName="${originTdpPath}-Linux-${cpuType}${packageLite}.${packageType}"

if [ "$testFile" == "server" ] ;then
    installCmd="install.sh"
elif [ ${testFile} = "client" ];then
    installCmd="install_client.sh"    
elif [ ${testFile} = "tools" ];then
    tdPath="taosTools-${version}"
    originTdpPath="taosTools-${originversion}"
    packageName="${tdPath}-Linux-${cpuType}${packageLite}-comp3.${packageType}"
    originPackageName="${originTdpPath}-Linux-${cpuType}${packageLite}.${packageType}"    
    installCmd="install-tools.sh"
fi


echo "tdPath:${tdPath},originTdpPath:${originTdpPath},packageName:${packageName},originPackageName:${originPackageName}"
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

function wgetFile {
    file=$1
    versionPath=$2
    sourceP=$3
    nasServerIP="192.168.1.213"
    packagePath="/nas/TDengine/v${versionPath}/${verMode}"
    if [ -f  ${file}  ];then
        echoColor  YD "${file} already exists ,it will delete it and download  it again "
        rm -rf ${file}
    fi

    if [[ ${sourceP} = 'web' ]];then
        echoColor  BD "====download====:wget https://www.taosdata.com/assets-download/3.0/${file}"
        wget https://www.taosdata.com/assets-download/3.0/${file}
    elif [[ ${sourceP} = 'nas' ]];then
        echoColor  BD "====download====:scp root@${nasServerIP}:${packagePath}/${file} ."
        scp root@${nasServerIP}:${packagePath}/${file} .
    fi
}

function newPath {
    buildPath=$1

    if [ ! -d ${buildPath} ] ;then
        echoColor BD "mkdir -p ${buildPath}"
        mkdir -p ${buildPath}
    else  
        echoColor YD "${buildPath} already exists"
    fi
}


echoColor G "===== install basesoft ====="

cmdInstall tree
cmdInstall wget
cmdInstall expect

echoColor G "===== Uninstall all components of TDeingne ====="

if command -v rmtaos ;then
    echoColor YD "uninstall all components of TDeingne:rmtaos"
    echo "n" | rmtaos 
else 
     echoColor YD "os doesn't include TDengine"
fi

if [[ -e /etc/os-release ]]; then
  osinfo=$(cat /etc/os-release | grep "NAME" | cut -d '"' -f2) || :
else
  osinfo=""
fi

if echo $osinfo | grep -qwi "ubuntu"; then
  #  echo "This is ubuntu system"
  apt remove tdengine -y
elif echo $osinfo | grep -qwi "centos"; then
  #  echo "This is centos system"
  yum remove tdengine -y
fi


if command -v rmtaostools ;then
    echoColor YD "uninstall all components of TDeingne:rmtaostools"
    rmtaostools
else 
    echoColor YD "os doesn't include rmtaostools "
fi

echoColor BD " pkill -9 taosd "
pkill -9 taosd


echoColor G "===== new workroom path ====="
installPath="/usr/local/src/packageTest"
oriInstallPath="/usr/local/src/packageTest/3.1"

newPath ${installPath}

newPath ${oriInstallPath}


if [ -d ${oriInstallPath}/${originTdpPath} ] ;then
    echoColor BD "rm -rf ${oriInstallPath}/${originTdpPath}/*"
    rm -rf  ${oriInstallPath}/${originTdpPath}/*  
fi

if [ -d ${installPath}/${tdPath} ] ;then
    echoColor BD "rm -rf ${installPath}/${tdPath}/*"
    rm -rf ${installPath}/${tdPath}/*
fi

echoColor G "===== download  installPackage ====="
cd ${installPath} && wgetFile ${packageName} ${version}  ${sourcePath}
cd  ${oriInstallPath}  && wgetFile ${originPackageName} ${originversion}   ${sourcePath}


cd ${installPath}
cp -r ${scriptDir}/debRpmAutoInstall.sh   . 

packageSuffix=$(echo ${packageName}  | awk -F '.' '{print $NF}')


if [ ! -f  debRpmAutoInstall.sh  ];then
    echo '#!/usr/bin/expect ' >  debRpmAutoInstall.sh
    echo 'set packageName [lindex $argv 0]' >>  debRpmAutoInstall.sh
    echo 'set packageSuffix [lindex $argv 1]' >>  debRpmAutoInstall.sh
    echo 'set timeout 30 ' >>  debRpmAutoInstall.sh
    echo 'if { ${packageSuffix} == "deb" } {' >>  debRpmAutoInstall.sh
    echo '    spawn  dpkg -i ${packageName} '  >>  debRpmAutoInstall.sh
    echo '} elseif { ${packageSuffix} == "rpm"} {' >>  debRpmAutoInstall.sh
    echo '    spawn rpm -ivh ${packageName}'  >>  debRpmAutoInstall.sh
    echo '}' >>  debRpmAutoInstall.sh
    echo 'expect "*one:"' >>  debRpmAutoInstall.sh
    echo 'send  "\r"' >>  debRpmAutoInstall.sh
    echo 'expect "*skip:"' >>  debRpmAutoInstall.sh
    echo 'send  "\r" ' >>  debRpmAutoInstall.sh
fi


echoColor G "===== instal Package ====="

if [[ ${packageName} =~ "deb" ]];then
    cd ${installPath}
    dpkg -r taostools
    dpkg -r tdengine
    if [[ ${packageName} =~ "TDengine" ]];then
        echoColor BD "./debRpmAutoInstall.sh ${packageName}  ${packageSuffix}" &&   chmod 755 debRpmAutoInstall.sh &&  ./debRpmAutoInstall.sh  ${packageName}  ${packageSuffix}
    else
        echoColor BD "dpkg  -i ${packageName}" &&   dpkg  -i ${packageName}
    fi
elif [[ ${packageName} =~ "rpm" ]];then
    cd ${installPath}
    sudo rpm -e tdengine
    sudo rpm -e taostools
    if [[ ${packageName} =~ "TDengine" ]];then
        echoColor BD "./debRpmAutoInstall.sh ${packageName}  ${packageSuffix}" &&   chmod 755 debRpmAutoInstall.sh &&  ./debRpmAutoInstall.sh  ${packageName}  ${packageSuffix}
    else
        echoColor BD "rpm  -ivh ${packageName}" &&   rpm  -ivh ${packageName}
    fi
elif [[ ${packageName} =~ "tar" ]];then
    echoColor G "===== check installPackage File of tar ====="
    cd  ${oriInstallPath}
    if [ ! -f  {originPackageName}  ];then
        echoColor YD "download  base installPackage"
        wgetFile ${originPackageName} ${originversion} ${sourcePath} 
    fi
    echoColor YD "unzip the base installation package" 
    echoColor BD "tar -xf ${originPackageName}" && tar -xf ${originPackageName} 
    cd ${installPath} 
    echoColor YD "unzip the new installation package" 
    echoColor BD "tar -xf ${packageName}" && tar -xf ${packageName} 

    if [ ${testFile} != "tools" ] ;then
        cd ${installPath}/${tdPath} && tar xf ${subFile}
        cd  ${oriInstallPath}/${originTdpPath}  && tar xf ${subFile}
    fi

    cd  ${oriInstallPath}/${originTdpPath} && tree -I "driver" >  ${installPath}/base_${originversion}_checkfile
    cd ${installPath}/${tdPath}   && tree -I "driver" > ${installPath}/now_${version}_checkfile
    
    cd ${installPath} 
    diff  ${installPath}/base_${originversion}_checkfile   ${installPath}/now_${version}_checkfile  > ${installPath}/diffFile.log
    diffNumbers=`cat ${installPath}/diffFile.log |wc -l `

    if [ ${diffNumbers} != 0 ];then
        echoColor R "The number and names of files is different from the previous installation package"
        diffLog=`cat ${installPath}/diffFile.log`
        echoColor Y "${diffLog}"
        exit -1
    else 
        echoColor G "The number and names of files are the same as previous installation packages"
        rm -rf ${installPath}/diffFile.log
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

if [[ ${packageName} =~ "Lite" ]]  ||   ([[ ${packageName} =~ "x64" ]] && [[ ${packageName} =~ "client" ]]) ||  ([[ ${packageName} =~ "deb" ]] && [[ ${packageName} =~ "server" ]])  || ([[ ${packageName} =~ "rpm" ]] && [[ ${packageName} =~ "server" ]]) ;then
    echoColor G "===== install taos-tools when package is lite or client ====="
    cd ${installPath}
    if [ ! -f "taosTools-2.5.4-Linux-x64.tar.gz " ];then
        wgetFile taosTools-2.5.3-Linux-x64-comp3.tar.gz v2.5.3 web
        tar xf taosTools-2.5.3-Linux-x64-comp3.tar.gz  
    fi
    cd taosTools-2.5.3 && bash install-tools.sh
elif  ([[ ${packageName} =~ "arm64" ]] && [[ ${packageName} =~ "client" ]]);then
    echoColor G "===== install taos-tools arm when package is arm64-client ====="
    cd ${installPath}
    if [ ! -f "taosTools-2.5.3-Linux-x64-comp3.tar.gz " ];then
        wgetFile taosTools-2.5.3-Linux-arm64-comp3.tar.gz v2.5.3 web
        tar xf taosTools-2.5.3-Linux-arm64-comp3.tar.gz
    fi    
    
    cd taosTools-2.5.3 && bash install-tools.sh
fi

echoColor G  "===== start TDengine ====="

if [[ ! ${packageName} =~ "client" ]] ;then
    echoColor BD " rm -rf /var/lib/taos/* &&  systemctl restart taosd "
    rm -rf /var/lib/taos/*
    systemctl restart taosd
fi

rm -rf ${installPath}/${packageName}
rm -rf ${installPath}/${tdPath}/

# if ([[ ${packageName} =~ "Lite" ]] &&  [[ ${packageName} =~ "tar" ]]) ||   [[ ${packageName} =~ "client" ]] ;then
#     echoColor G "===== install taos-tools when package is lite or client ====="
#     cd ${installPath}
#     wgetFile taosTools-2.1.2-Linux-x64.tar.gz .
#     tar xf taosTools-2.1.2-Linux-x64.tar.gz
#     cd taosTools-2.1.2 && bash install-taostools.sh
# elif [[ ${packageName} =~ "Lite" ]] &&  [[ ${packageName} =~ "deb" ]] ;then
#     echoColor G "===== install taos-tools when package is lite or client ====="
#     cd ${installPath}
#     wgetFile taosTools-2.1.2-Linux-x64.tar.gz .
#     tar xf taosTools-2.1.2-Linux-x64.tar.gz
#     cd taosTools-2.1.2 && bash install-taostools.sh
# elif [[ ${packageName} =~ "Lite" ]] &&  [[ ${packageName} =~ "rpm" ]]  ;then
#     echoColor G "===== install taos-tools when package is lite or client ====="
#     cd ${installPath}
#     wgetFile taosTools-2.1.2-Linux-x64.tar.gz .
#     tar xf taosTools-2.1.2-Linux-x64.tar.gz
#     cd taosTools-2.1.2 && bash install-taostools.sh
# fi

