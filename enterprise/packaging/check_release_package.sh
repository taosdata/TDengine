#! /bin/sh
set -e

RED="\e[1;31m"
GREEN="\e[1;32m"
YELLOW="\e[1;33m"
BLUE="\e[1;34m"
PINK="\e[1;35m"
RES="\e[0m"

VERSION="$1"  #"3.9.9.9"
MODE="$2"   #"edge"
CPU="$3"    #"Linux-x64"
PKG_HOME="/nas/TDengine/v${VERSION}"

PKG_NAME_COM_X64="TDengine-server-${VERSION}-${CPU}.tar.gz"
PKG_NAME_ENT_X64="TDengine-enterprise-server-${VERSION}-${CPU}.tar.gz"

PKG_NAME_COM_ARM64="TDengine-server-${VERSION}-${CPU}.tar.gz"
PKG_NAME_ENT_ARM64="TDengine-enterprise-server-${VERSION}-${CPU}.tar.gz"

PKG_NAME_COM_MIPS64="TDengine-server-${VERSION}-${CPU}.tar.gz"
PKG_NAME_ENT_MIPS64="TDengine-enterprise-server-${VERSION}-${CPU}.tar.gz"

FOLDER_NAME="TDengine-server-${VERSION}"
FOLDER_NAME_ENT="TDengine-enterprise-server-${VERSION}"
OS_NAME=`cat /etc/os-release |grep ^NAME=|awk -F '=' '{print $2}'|awk -F '"' '{print $2}'`

print(){
    if [ "${OS_NAME}" = "Ubuntu" ]
    then
        echo $1
        echo ""
    elif [ "${OS_NAME}" = "CentOS Linux" ]
    then
        echo -e $1
        echo ""
    else
        echo $1
        echo ""
    fi
}

StartTaosd(){
    sudo systemctl start taosd
    sudo systemctl status taosd
    # check client status

    sudo systemctl start taosadapter
    sudo systemctl status taosadapter
    # check taosadapter status

}

CheckVersion(){
    CLIENT_VERSION=`taos -s "select client_version()"|sed -n '7,7p'|awk '{print $1}'`
    SERVER_VERSION=`taos -s "select server_version()"|sed -n '7,7p'|awk '{print $1}'`
    print "${GREEN}[client_version]:${CLIENT_VERSION} ${RES}"

    if [ ${CLIENT_VERSION} = "${VERSION}" -a ${CLIENT_VERSION} != ""  ]
    then
        print "${GREEN}client version through taos-shell correct ${RES}"
    else
        print "${RED}client version ${CLIENT_VERSION} through tao-shell is incorrect ${RES}"
        exit 1
    fi

    if [ ${SERVER_VERSION} = "${VERSION}" -a ${SERVER_VERSION} != ""  ]
    then
        print "${GREEN}server version through taos-shell correct${RES}"
    else
        print "${RED}server version ${SERVER_VERSION} through tao-shell is incorrect${RES}"
        exit 1
    fi

    CLIENT_VERSION_ADP=`curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ=="  -d "select client_version()"   127.0.0.1:6041/rest/sql|jq -r '.data[0][0]'`
    SERVER_VERSION_ADP=`curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ=="  -d "select server_version()"   127.0.0.1:6041/rest/sql|jq -r '.data[0][0]'`

    if [ ${CLIENT_VERSION_ADP} = "${VERSION}" -a ${CLIENT_VERSION_ADP} != ""  ]
    then
        print "${GREEN}client version through taosadpter correct${RES}"
    else
        print "${RED}client version ${CLIENT_VERSION_ADP} through tao-shell is incorrect${RES}"
        exit 1
    fi


    if [ ${SERVER_VERSION_ADP} = "${VERSION}" -a ${SERVER_VERSION_ADP} != ""  ]
    then
        print "${GREEN}server version through taosadpter correct${RES}"
    else
        print "${RED}server version ${SERVER_VERSION_ADP} through tao-shell is incorrect${RES}"
        exit 1
    fi
}

CheckEnterpriseBinFiles(){
    # clean package
    print `pwd`
    if [ ! -e remove.sh ]
    then
        print "${RED}remove.sh not found in this enterprise package ${RES}"
        exit 1
    fi
    if [ ! -e set_core.sh ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e startPre.sh ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e taos ]
    then
        print "${RED} not found in this enterprise package${RES}"
t        exit 1
    fi
    if [ ! -e taosadapter ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e taosBenchmark ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e taosd ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e taosd-dump-cfg.gdb ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e taosdump ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e taoskeeper ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e taosx ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e tdengine-datasource.zip ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e tdengine-datasource.zip.md5sum ]
    then
        print "${RED} not found in this enterprise package${RES}"
        exit 1
    fi
    if [ ! -e TDinsight.sh ]
    then
        print "${RED}TDinsight.sh not find in package${RES}"
        exit 1
    fi
    if [ ! -e udfd ]
    then
        print "${RED}udfd not find in package${RES}"
        exit 1
    fi
    print "${GREEN} check enterprise package's bin folder correct.${RES}"


    if [ ! -e taos-explorer ]
    then
        print "${RED}taos-explorer not find in package${RES}"
        exit 1
    fi
    print "${GREEN} check enterprise package's bin folder correct.${RES}"
}


CheckCommunityBinFiles(){
    print `pwd`
    if [ ! -e remove.sh ]
    then
        print "${RED}remove.sh not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e set_core.sh ]
    then
        print "${RED}set_core.sh not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e startPre.sh ]
    then
        print "${RED}startPre.sh not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e taos ]
    then
        print "${RED}taos not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e taosadapter ]
    then
        print "${RED}taosadapter not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e taosBenchmark ]
    then
        print "${RED}taosBenchmark not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e taosd ]
    then
        print "${RED}taosd not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e taosd-dump-cfg.gdb ]
    then
        print "${RED}taosd-dump-cfg.gdb not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e taosdump ]
    then
        print "${RED}taosdump not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e taoskeeper ]
    then
        print "${RED}taoskeeper not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e tdengine-datasource.zip ]
    then
        print "${RED}tdengine-datasource.zip not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e tdengine-datasource.zip.md5sum ]
    then
        print "${RED}tdengine-datasource.zip.md5sum not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e TDinsight.sh ]
    then
        print "${RED}TDinsight.sh not found in this conmmunity package ${RES}"
        exit 1
    fi
    if [ ! -e udfd ]
    then
        print "${RED}udfd not found in this conmmunity package ${RES}"
        exit 1
    fi
    print "${GREEN}check community package's bin folder done.${RES}"

}


CleanEnv(){
    if [ -L /usr/bin/taos ]
    then
        print "${GREEN}taos will be removed.${RES}"
        sudo rmtaos
    fi

    # remove /var/lib/taos/*
    if [ -e /var/lib/taos ]
    then
        print "${GREEN}removing /var/lib/taos/*.${RES}"
        sudo rm -rf /var/lib/taos/*
    fi

    print "try to remove unziped folder"

    # community
    if [ -d ${PKG_HOME}/community/${FOLDER_NAME} ]
    then
        print "${GREEN} remove ${PKG_HOME}/community/${FOLDER_NAME}${RES}"
        sudo rm -rf ${PKG_HOME}/community/${FOLDER_NAME}
    fi

    # enterprise
    if [ -d ${PKG_HOME}/enterprise/${FOLDER_NAME} ]
    then
        print "${GREEN} remove ${PKG_HOME}/enterprise/${FOLDER_NAME}${RES}"
        sudo rm -rf ${PKG_HOME}/enterprise/${FOLDER_NAME}
    fi


}

TestArm64(){
    if [ "${MODE}" != "cluster" -a "${MODE}" != "edge"  ] && [ "${MODE}" != "all" ]
    then
        print "${RED}invalid mode type, only accept edge,cluste and all.${RES}"
        exit 1
    fi

    if [ ${MODE} = "edge" -o ${MODE} = "all" ]
    then
        print "${YELLOW} package ${PKG_NAME_COM_ARM64} will be tested.${RES}"
        CleanEnv
        cd ${PKG_HOME}/community/
        sudo tar -zxvf  ${PKG_NAME_COM_ARM64}
        cd ${FOLDER_NAME}
        sudo ./install.sh -e no
        tar -zxvf package.tar.gz
        cd bin

        StartTaosd
        CheckVersion
        CheckCommunityBinFiles
        print "${GREEN}check ${PKG_NAME_COM_ARM64} package done.${RES}"

    fi
    if [ ${MODE} = "cluster" -o ${MODE} = "all" ]
    then
        print "${YELLOW} package ${PKG_NAME_ENT_ARM64} will be tested.${RES}"
        CleanEnv
        cd ${PKG_HOME}/enterprise/
        sudo tar -zxvf  ${PKG_NAME_ENT_ARM64}
        cd ${FOLDER_NAME_ENT}
        sudo ./install.sh -e no
        tar -zxvf package.tar.gz
        cd bin

        StartTaosd
        CheckVersion
        CheckEnterpriseBinFiles
        print "${GREEN}check ${PKG_NAME_ENT_ARM64} package done.${RES}"
    fi
}

TestX64(){
    if [ "${MODE}" != "cluster" -a "${MODE}" != "edge"  ] && [ "${MODE}" != "all" ]
    then
        print "${RED}invalid mode type, only accept edge,cluste and all.${RES}"
        exit 1
    fi

    if [ "$MODE" = "edge" -o "$MODE" = "all" ]
    then
        print "${YELLOW} package ${PKG_NAME_COM_X64} will be tested.${RES}"
        CleanEnv
        cd ${PKG_HOME}/community
        sudo tar -zxvf  ${PKG_NAME_COM_X64}
        cd ${FOLDER_NAME}
        sudo ./install.sh -e no
        tar -zxvf package.tar.gz
        cd bin

        StartTaosd
        CheckVersion
        CheckCommunityBinFiles
        print "${GREEN}check ${PKG_NAME_COM_X64} package done.${RES}"
    fi
    if [ "${MODE}" = "cluster" -o "${MODE}" = "all" ]
    then
        print "${YELLOW}package ${PKG_NAME_ENT_X64} will be tested.${RES}"
        CleanEnv
        cd ${PKG_HOME}/enterprise/
        sudo tar -zxvf  ${PKG_NAME_ENT_X64}
        cd ${FOLDER_NAME_ENT}
        sudo ./install.sh -e no
        tar -zxvf taos.tar.gz
        cd bin

        StartTaosd
        CheckVersion
        CheckEnterpriseBinFiles
        print "${GREEN}check ${PKG_NAME_ENT_X64} package done.${RES}"
    fi
}

TestMips64(){
    if [ "${MODE}" != "cluster" -a "${MODE}" != "edge"  ] && [ "${MODE}" != "all" ]
    then
        print "${RED}invalid mode type, only accept edge,cluste and all.${RES}"
        exit 1
    fi

    if [ "$MODE" = "edge" -o "$MODE" = "all" ]
    then
        print "${YELLOW} package ${PKG_NAME_COM_MIPS64} will be tested.${RES}"
        CleanEnv
        cd ${PKG_HOME}/community
        sudo tar -zxvf  ${PKG_NAME_COM_MIPS64}
        cd ${FOLDER_NAME}
        sudo ./install.sh -e no
        tar -zxvf taos.tar.gz
        cd bin

        StartTaosd
        CheckVersion
        CheckCommunityBinFiles
        print "${GREEN}check ${PKG_NAME_COM_MIPS64} package done.${RES}"
    fi
    if [ "${MODE}" = "cluster" -o "${MODE}" = "all" ]
    then
        print "${YELLOW}Currenly MIPS enterprise package ${PKG_NAME_ENT_MIPS64} in not supported, skip....${RES}"
    fi
}
# TestDarwinX64(){}
# TestDarwinArm64(){}


# Linux-x64
if [  ${CPU} = "Linux-x64"  ]
then
    # print "${GREEN} Linux x64 package will be tested.${RES}"
    print "${GREEN}Linux x64 package will be tested.${RES}"
    TestX64
fi
# Linux arm64
if [ ${CPU} = "Linux-arm64" ]
then
    print "${GREEN}Linux ARM64 package will be tested.${RES}"
    TestArm64
fi

# MIPS 64
if [ ${CPU} = "Linux-mips64" -a "${OS_NAME}" = "Kylin" ]
then

    print "${GREEN}Linux mips64 package will be tested.${RES}"
    TestMips64
fi
# clean environment
# CleanEnv
