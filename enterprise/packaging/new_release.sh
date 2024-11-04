#!/bin/bash

#set -e

baseDir=/data/release
branch=main
pagMode=full
versionType=enterprise
grantValue=10
version=""
verNumberComp="3.0.0.0"
dockerMode="no"
os_type=""
os_arch=""
prefix=taos
productName=TDengine
productEmail=support@taosdata.com
packageType=all      #[all, server, client]
industryName=Power
commitCheck=false

while getopts "hb:n:l:I:v:d:N:P:M:D:G:c:" arg
do
  case $arg in    
    b)
      #echo "branchName=$OPTARG"
      branch=$(echo $OPTARG)
      ;;
    n)
      #echo "version=$OPTARG"
      version=$(echo $OPTARG)
      ;;
    l)
      #echo "pagMode=$OPTARG"
      pagMode=$(echo $OPTARG)
      ;;    
    I)
      #echo "industryName=$OPTARG"
      industryName=$(echo $OPTARG)
      ;;
    v)
      #echo "versionType=$OPTARG"
      versionType=$(echo $OPTARG)
      ;;    
    d)
      #echo "dockerMode=$OPTARG"
      dockerMode=$(echo $OPTARG)
      ;;
    N)
      #echo "cusName=$OPTARG"
      cusName=$(echo $OPTARG)
      ;;
    P)
      #echo "cusPrompt=$OPTARG"
      prefix=$(echo $OPTARG)
      ;;
    M)
      #echo "cusEmail=$OPTARG"
      cusEmail=$(echo $OPTARG)
      ;;
    D)
      #echo "cusEmail=$OPTARG"
      baseDir=$(echo $OPTARG)
      ;;  
    G)
      grantValue=$(echo $OPTARG)
      ;;
    c)
      commitCheck=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -b [main | 3.1] "
      echo "                     -n [version number: 3.1.*.* | 3.2.*.* ]"
      echo "                     -l [full | lite]  "      
      echo "                     -I [industryName: Power | Gasoline ]  "
      echo "                     -v [enterprise, community ,all]"
      echo "                     -d [no | build | push | latest]"
      echo "                     -N <custom name>"
      echo "                     -P <custom prompt>"
      echo "                     -M <custom email>"      
      echo "                     -G <grant days>"
      echo "                     -c [true | false]"
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

version_pattern='^[0-9]+\.([0-9]+\.){1,3}[0-9]+$'
if [ -z "$version" ]; then
    echo "Version number is empty, please specify version number"
    exit 1
elif [ ${#version} -ge 16 ]; then
    echo "Length of the version number should be less than 16"
    exit 1
elif [[ ! $version =~ $version_pattern ]]; then
    echo "Only digits and dots are allowed in the version number, alpha characters are not allowed"
    exit 1
fi

internalDir="${baseDir}/${branch}/TDinternal"
communityDir="${internalDir}/community"
connectorDir="${baseDir}/${branch}/connector"
exampleDir="${communityDir}/examples"
taosxDir="${baseDir}/${branch}/taosx"
explorerDir="${baseDir}/${branch}/explorer"
install_dir="${baseDir}/${branch}/install_dir"

if [ "$versionType" == "community" ]; then
    serverPackageName="${productName}-server-${version}"
    clientPackageName="${productName}-client-${version}"
    debugDir="${communityDir}/debug"
    keeperDir="${baseDir}/${branch}/taoskeeper"
    archiveDir="/nas/TDengine/v${version}/community"
    cfg_dir="${communityDir}/packaging/cfg"
else
    if [ "$versionType" == "enterprise" ]; then 
        serverPackageName="${productName}-enterprise-${version}"
        clientPackageName="${productName}-enterprise-client-${version}"
    elif [ "$versionType" == "industry" ]; then
        serverPackageName="${productName}-${industryName}-${version}"
        clientPackageName="${productName}-${industryName}-client-${version}"
    fi
    debugDir="${internalDir}/debug"    
    keeperDir="${baseDir}/${branch}/taoskeeperinternal"
    archiveDir="/nas/TDengine/v${version}/enterprise"
    cfg_dir="${internalDir}/enterprise/packaging/cfg"
fi

ostype=`uname`
# get number of CORES
if [ "${ostype}" == "Darwin" ]; then
    CORES=$(sysctl -n hw.ncpu)
elif [ "${ostype}" == "Linux" ] && [ "${cpuType}" == "arm64" ]; then
    CORES=1
else
    CORES=$(grep -c ^processor /proc/cpuinfo)
fi

uname_info=`uname -a`
# get os type
if echo $uname_info | grep -q "Linux"; then
    os_type=Linux
elif echo $uname_info | grep -q "Darwin"; then
    os_type=Darwin
fi

# get os arch
if echo $uname_info | grep -q "x86_64"; then
    os_arch=x64
elif echo $uname_info | grep -q "arm64\|aarch64"; then
    os_arch=arm64
fi

echo "Build TDengine package on ${os_type} ${os_arch}"
echo "execute new_release.sh -b ${branch} -n ${version} -D ${baseDir} -l ${pagMode} -v ${versionType} -d ${dockerMode} -M ${cusEmail} -G ${grantValue} -c ${commitCheck}"

# connectors
connectors=()
connectors+=(taos-connector-jdbc)
connectors+=(driver-go)
connectors+=(taos-connector-python)
connectors+=(taos-connector-node)
connectors+=(taos-connector-dotnet)
connectors+=(taos-connector-rust)

function git_pull() {
    if [ -d $1 ]; then
        cd $1
        echo "change directory to $1 done"
        
        git reset --hard HEAD
        echo "$1: reset to HEAD done"

        git checkout $2 || :
        echo "$1: checkout branch $2 done"

        git pull
        echo "$1: pull latest code done"
        
        if git tag -d $3 > /dev/null 2>&1; then
            echo "$1: delete old tag $3 done"
        fi        

        if [[ "$3" != "main" && "$3" != "3.1" && "$3" != "3.0"  ]]; then
            if git branch -D $3 > /dev/null 2>&1; then
                echo "$1: delete old branch $3 done"
            fi
        fi
        
        for ((num=1; num<=5; num++))
        do
            git fetch --all
            if git checkout $3 > /dev/null 2>&1; then                
                break
            else
                sleep 2
            fi

            if [ $num -eq 5 ]; then
                echo "$1: checkout branch/tag $3 failed"                
            fi
        done
    fi
}

function industry_options() {
    local options=""
    TD_INDUSTRY_NAME="TDengine ${industryName} Edition"
    if [[ "$industryName" == "Power" && "$versionType" == "industry" ]]; then        
        options="-DTD_INDUSTRY=true \
            -DTD_PRODUCT_NAME=\"${TD_INDUSTRY_NAME}\" \
            -DTD_FUNC_STREAM=false \
            -DTD_FUNC_SUBSCRIPTION=false \
            -DTD_FUNC_AUDIT=false \            
            -DTD_FUNC_VIEW=false \
            -DTD_FUNC_MULTI_TIER_STORAGE=false \
            -DTD_FUNC_DATA_BAK_RESTORE=false \
            -DTD_FUNC_OBJECT_STORAGE=false \
            -DTD_FUNC_ACTIVE_ACTIVE=false \
            -DTD_FUNC_DUAL_REPLICA_HA=false \
            -DTD_FUNC_DB_ENCRYPTION=false \
            -DTD_FUNC_DATA_SYNC=false \
            -DTD_DATAIN_OPC_DA=false \
            -DTD_DATAIN_OPC_UA=false \
            -DTD_DATAIN_PI=false \
            -DTD_DATAIN_KAFKA=false \
            -DTD_DATAIN_INFLUXDB=false \
            -DTD_DATAIN_MQTT=false \
            -DTD_DATAIN_AVEVAHISTORIAN=false \
            -DTD_DATAIN_OPENTSDB=false \
            -DTD_DATAIN_TDENGINE_2_6=false \
            -DTD_DATAIN_TDENGINE_3_0=false \
            -DTD_DATAIN_MYSQL=false \
            -DTD_DATAIN_POSTGRES=false \
            -DTD_DATAIN_ORACLE=false \
            -DTD_DATAIN_MSSQL=false \
            -DTD_DATAIN_MONGODB=false \
            -DTD_DATAIN_CSV=false"
    fi
    echo $options
}

function build_TDengine() {
    echo "build TDengine"
    build_time=$(date +"%F %T %z")
    
    # get TDinternal commit id
    if [ "$versionType" != "community" ]; then
        cd ${internalDir}
        git_pull "${internalDir}" "${branch}" "release/ver-${version}"
        gitinfoOfInternal=$(git rev-parse --verify HEAD)

        # pull grant-lib
        git_pull "${internalDir}/enterprise/contrib/grant-lib" "${branch}" "ver-${version}"
    else
        gitinfoOfInternal=NULL
    fi

    # get TDengine commit id
    if [ -d ${communityDir} ]; then
        cd ${communityDir}
        git_pull ${communityDir} ${branch} release/ver-${version}
        gitinfo=$(git rev-parse --verify HEAD)

        # pull taosadapter        
        git_pull "${communityDir}/tools/taosadapter" "${branch}" "ver-${version}"

        # pull taos-tools
        git_pull "${communityDir}/tools/taos-tools" "main" "ver-${version}"

        # pull taosws-rs
        git_pull "${communityDir}/tools/taosws-rs" "main" "main"

    else
        echo "can not find TDengine source code"
        exit 1
    fi

    binPath=""
    repoDir=""
    if [ "$versionType" != "community" ];then
        
        mkdir -p ${internalDir}/debug
        cd ${internalDir}/debug
        cmd="cmake ../ -DCMAKE_BUILD_TYPE=Release -DASSERT_NOT_CORE=true -DCPUTYPE=${os_arch} -DWEBSOCKET=true -DOSTYPE=${os_type} -DSOMODE=dynamic -DDBNAME=taos -DVERTYPE=stable -DVERDATE=\"${build_time}\" -DGITINFO=${gitinfo} -DGITINFOI=${gitinfoOfInternal} -DVERNUMBER=${version} -DVERCOMPATIBLE=3.0.0.0 -DBUILD_HTTP=internal -DBUILD_TOOLS=true -DGRANT_VALUE=${grantValue}"
        if [ "$versionType" == "enterprise" ]; then
            echo "build TDengine enterprise version"
            echo $cmd
            eval $cmd
        elif [ "$versionType" == "industry" ]; then
            echo "build TDengine industry version"
            industry_cmd="$cmd $(industry_options)"
            echo $industry_cmd
            eval $industry_cmd
        fi
        binPath="${internalDir}/debug/build/bin"
        repoDir="${internalDir}"
    else
        echo "build TDengine community version"
        mkdir -p ${communityDir}/debug
        cd ${communityDir}/debug
        echo "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCPUTYPE=${os_arch} -DWEBSOCKET=true -DOSTYPE=${os_type} -DSOMODE=dynamic -DDBNAME=taos -DVERTYPE=stable -DVERDATE='${build_time}' -DGITINFO=${gitinfo} -DVERNUMBER=${version} -DVERCOMPATIBLE=3.0.0.0 -DBUILD_HTTP=false -DBUILD_TOOLS=true"
        cmake ../ -DCMAKE_BUILD_TYPE=Release -DCPUTYPE=${os_arch} -DWEBSOCKET=true -DOSTYPE=${os_type}          \
            -DSOMODE=dynamic -DDBNAME=taos -DVERTYPE=stable -DVERDATE="${build_time}" -DGITINFO=${gitinfo}        \
            -DVERNUMBER=${version} -DVERCOMPATIBLE=3.0.0.0 -DBUILD_HTTP=false -DBUILD_TOOLS=true
        binPath="${communityDir}/debug/build/bin"
        repoDir="${communityDir}"
    fi
    
    if [ $? -ne 0 ]; then
        echo "cmake failed"
        exit 1
    fi

    make -j $CORES && make install
    if [ $? -ne 0 ]; then
        echo "make failed"
        exit 1
    fi
    
    verify_commit_id $binPath taosd $repoDir
    verify_commit_id $binPath taosadapter $communityDir/tools/taosadapter
    verify_commit_id $binPath taosdump $communityDir/tools/taos-tools
    verify_commit_id $binPath taosBenchmark $communityDir/tools/taos-tools
}

function build_taosx() {
    if [ "$versionType" != "community" ] && [ "${os_type}" != "Darwin" ]; then
        echo "build taosx"
        rm -rf ${taosxDir}/release/* || :        

        # pull taosx
        git_pull "${taosxDir}" "main" "ver-$version"

        rm -rf ${taosxDir}/target/taosx.dev.db
        rm -rf ${taosxDir}/.new
        cd ${taosxDir}/packaging
        if [ "$versionType" == "industry" ]; then
            echo "export VUE_APP_INDUSTRY=${industryName}"
            export VUE_APP_INDUSTRY=${industryName}
        fi
        export CFLAGS="-fno-builtin"
        python3 release.py -ob -vn $version
        if [ -f "${taosxDir}/release/taosx/bin/taosx" ] && [ -f "${taosxDir}/release/taosx/bin/taos-explorer" ]; then
            echo "build taosx done"
        else
            echo "build taosx failed"
            exit 1
        fi

        verify_commit_id "$taosxDir/release/taosx/bin"  "taosx"  "$taosxDir"
        verify_commit_id "$taosxDir/release/taosx/bin"  "taos-explorer"  "$taosxDir"
    else
        echo "build explorer community version"
        rm -rf ${explorerDir}/target/release/taos-explorer || :
        rm -rf ${explorerDir}/public/docs ${explorerDir}/public/docs-en || true
        # pull explorer
        git_pull "${explorerDir}" "main" "ver-$version"
        cd ${explorerDir}
        export VUE_APP_COMMUNITY=community
        yarn
        VER_NUMBER=$version yarn build:bin
        if [ -f "${explorerDir}/target/release/${prefix}-explorer" ]; then
            echo "build explorer community version done"
        else
            echo "build explorer community version failed"
            exit 1
        fi

        verify_commit_id $explorerDir/target/release taos-explorer $explorerDir
    fi
}

function build_taoskeeper() {
    echo "build taoskeeper"

    # skip build taoskeeper for enterprise version on Mac
    if [ "${os_type}" == "Darwin" ] && [ "$versionType" == "enterprise" ]; then
        return
    fi
    
    platform="${os_type}-${os_arch}"
    dateinfo=`date +"%F %T %:z"`
    buildinfo="${platform} ${dateinfo}"    

    if [ "$versionType" != "community" ];then
        keeper_url="github.com/taosdata/taoskeeperinternal"
    else
        keeper_url="github.com/taosdata/taoskeeper"
    fi
    
    cd $keeperDir
    rm -rf taoskeeper || :
    git_pull $keeperDir ${branch} ver-${version}
    gitinfo=`git rev-parse HEAD`

    go build -ldflags="-s -w -X '${keeper_url}/version.Version=$version' -X '${keeper_url}/version.Gitinfo=$gitinfo' -X '${keeper_url}/version.BuildInfo=$buildinfo'" -o taoskeeper main.go
    
    if [ -f taoskeeper ]; then
        echo "build taoskeeper success"
    else
        echo "build taoskeeper failed"
        exit 1
    fi

    verify_commit_id $keeperDir taoskeeper $keeperDir
}

function update_connectors() {

    if [ "${os_type}" != "Darwin" ]; then
        for connector in "${connectors[@]}"; do
            cd ${connectorDir}/${connector}
            git pull

            if [ "${connector}" == "taos-connector-jdbc" ]; then
                mvn clean package -Dmaven.test.skip=true
            fi
        done

        cd ${connectorDir}
        rm -rf tdengine-datasource*
        rm -rf TDinsight.sh
        wget https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh && echo "TDinsight.sh downloaded!" \
        || echo "failed to download TDinsight.sh"
        
        chmod +x TDinsight.sh
        ./TDinsight.sh --download-only ||:
    fi
}

function preparepkg() {
    header_files="${communityDir}/include/client/taos.h ${communityDir}/include/common/taosdef.h ${communityDir}/include/util/taoserror.h ${communityDir}/include/util/tdef.h ${communityDir}/include/libs/function/taosudf.h"
    wsheader_files="${debugDir}/build/include/taosws.h"

    cd ${baseDir}/${branch}    
    rm -rf ${install_dir}/*
    mkdir -p ${install_dir}/bin ${install_dir}/cfg ${install_dir}/inc ${install_dir}/init.d

    # copy bin files
    serverBin=(${prefix} ${prefix}d ${prefix}adapter ${prefix}Benchmark ${prefix}dump udfd)

    for bin in "${serverBin[@]}"; do
        if [ -f "${debugDir}/build/bin/${bin}" ]; then
            cp ${debugDir}/build/bin/${bin} ${install_dir}/bin || :
        else
            echo "can not find ${bin} in ${debugDir}/build/bin"            
            exit 1
        fi
    done

    [ -f "${connectorDir}/TDinsight.sh" ] && cp ${connectorDir}/TDinsight.sh ${install_dir}/bin || :
    [ -f "${connectorDir}/tdengine-datasource.zip" ] && cp ${connectorDir}/tdengine-datasource.zip ${install_dir}/bin || :
    [ -f "${connectorDir}/tdengine-datasource.zip.md5" ] && cp ${connectorDir}/tdengine-datasource.zip.md5 ${install_dir}/bin || :
    
    [ -f "${communityDir}/packaging/tools/set_core.sh" ] && cp ${communityDir}/packaging/tools/set_core.sh ${install_dir}/bin || :
    [ -f "${communityDir}/packaging/tools/startPre.sh" ] && cp ${communityDir}/packaging/tools/startPre.sh ${install_dir}/bin || :
    [ -f "${communityDir}/packaging/tools/remove.sh" ] && cp ${communityDir}/packaging/tools/remove.sh ${install_dir}/bin || :
    [ -f "${communityDir}/packaging/tools/remove_client.sh" ] && cp ${communityDir}/packaging/tools/remove_client.sh ${install_dir}/bin || :
    [ -f "${communityDir}/packaging/tools/get_client.sh" ] && cp ${communityDir}/packaging/tools/get_client.sh ${install_dir}/bin || :
    [ -f "${communityDir}/packaging/tools/quick_deploy.sh" ] && cp ${communityDir}/packaging/tools/quick_deploy.sh ${install_dir}/bin || :
    [ -f "${communityDir}/packaging/tools/taosd-dump-cfg.gdb" ] && cp ${communityDir}/packaging/tools/taosd-dump-cfg.gdb ${install_dir}/bin/${prefix}d-dump-cfg.gdb || :
    
    [ -f "${keeperDir}/taoskeeper" ] && cp ${keeperDir}/taoskeeper ${install_dir}/bin || :
    chmod a+x ${install_dir}/bin/*

    # copy cfg files
    cp ${cfg_dir}/${prefix}.cfg ${install_dir}/cfg/ || :
    if [ -f "${communityDir}/packaging/cfg/${prefix}d.service" ]; then
        cp ${communityDir}/packaging/cfg/${prefix}d.service ${install_dir}/cfg || :
    fi

    if [ -f "${debugDir}/test/cfg/${prefix}adapter.toml" ]; then
        cp ${debugDir}/test/cfg/${prefix}adapter.toml ${install_dir}/cfg || :
    fi

    if [ -f "${debugDir}/test/cfg/${prefix}adapter.service" ]; then
        cp ${debugDir}/test/cfg/${prefix}adapter.service ${install_dir}/cfg || :
    fi

    if [ -f "${keeperDir}/config/${prefix}keeper.toml" ]; then
        cp ${keeperDir}/config/${prefix}keeper.toml ${install_dir}/cfg || :
    fi

    if [ -f "${keeperDir}/${prefix}keeper.service" ]; then
        cp ${keeperDir}/${prefix}keeper.service ${install_dir}/cfg || :
    fi

    # copy inc files
    cp ${header_files} ${install_dir}/inc
    [ -f ${wsheader_files} ] && cp ${wsheader_files} ${install_dir}/inc

    # copy init.d files
    cp ${communityDir}/packaging/deb/taosd ${install_dir}/init.d/${prefix}d.deb
    cp ${communityDir}/packaging/deb/taosd ${install_dir}/init.d/${prefix}d.rpm

    # copy drivers
    mkdir -p ${install_dir}/driver
    cp ${debugDir}/build/lib/libtaos.so.${version} ${install_dir}/driver || :
    cp ${debugDir}/build/lib/libtaosws.so ${install_dir}/driver || :
    echo "${verNumberComp}" > ${install_dir}/driver/vercomp.txt
    

    # copy examples
    echo "mkdir -p ${install_dir}/examples"
    mkdir -p ${install_dir}/examples
    cp -r ${exampleDir}/c ${install_dir}/examples || :
    cp -r ${exampleDir}/JDBC ${install_dir}/examples || :
    cp -r ${exampleDir}/matlab ${install_dir}/examples || :
    cp -r ${exampleDir}/python ${install_dir}/examples || :
    cp -r ${exampleDir}/R ${install_dir}/examples || :
    cp -r ${exampleDir}/go ${install_dir}/examples || :
    cp -r ${exampleDir}/nodejs ${install_dir}/examples || :
    cp -r ${exampleDir}/C# ${install_dir}/examples || :
    mkdir -p ${install_dir}/examples/taosbenchmark-json && cp ${exampleDir}/../tools/taos-tools/example/* ${install_dir}/examples/taosbenchmark-json || :

    # copy connectors
    mkdir -p ${install_dir}/connector
    cd ${install_dir}/connector
    cp -r ${connectorDir}/driver-go . && mv driver-go go && rm -rf go/.git || :
    cp -r ${connectorDir}/taos-connector-python . && mv taos-connector-python python && rm -rf python/.git || :
    cp -r ${connectorDir}/taos-connector-node . && mv taos-connector-node nodejs && rm -rf nodejs/.git || :
    cp -r ${connectorDir}/taos-connector-dotnet . && mv taos-connector-dotnet dotnet && rm -rf dotnet/.git || :
    cp -r ${connectorDir}/taos-connector-rust . && mv taos-connector-rust rust && rm -rf rust/.git || :
    
    cp ${connectorDir}/taos-connector-jdbc/target/*.jar . || :
       
    cd ${install_dir}
    if [ "${versionType}" != "community" ]; then
        # copy taosx         
        cp -r ${taosxDir}/release/taosx ${install_dir} || :        
        cp ${taosxDir}/packaging/uninstall.sh ${install_dir}/taosx/uninstall_taosx.sh || :
        sed -i "s/uninstall.sh/uninstall_taosx.sh/g" ${install_dir}/taosx/uninstall_taosx.sh
    else
        # copy explorer
        cp ${explorerDir}/target/release/${prefix}-explorer ${install_dir}/bin || :
        cp ${explorerDir}/target/${prefix}-explorer.service ${install_dir}/cfg || :
        cp ${explorerDir}/server/examples/explorer.toml ${install_dir}/cfg || :
    fi
    
    # copy others    
    cp ${internalDir}/enterprise/packaging/start-all.sh ${install_dir} || :
    cp ${internalDir}/enterprise/packaging/stop-all.sh ${install_dir} || :
    cp ${internalDir}/enterprise/packaging/README.md ${install_dir} || :
    cp ${communityDir}/packaging/tools/install.sh ${install_dir} || :
    cp ${communityDir}/packaging/tools/install_client.sh ${install_dir} || :
    if [ "$versionType" != "community" ]; then
        sed -i 's/verMode=edge/verMode=cluster/g' ${install_dir}/install.sh
        sed -i "s/PREFIX=\"taos\"/PREFIX=\"${prefix}\"/g" ${install_dir}/install.sh
        sed -i "s/productName=\"TDengine\"/productName=\"${productName}\"/g" ${install_dir}/install.sh
        cusDomain=`echo "${productEmail}" | sed 's/^[^@]*@//'`
        sed -i "s/emailName=\"taosdata.com\"/emailName=\"${cusDomain}\"/g" ${install_dir}/install.sh

        sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/bin/remove.sh
        sed -i "s/PREFIX=\"taos\"/PREFIX=\"${prefix}\"/g" ${install_dir}/bin/remove.sh
        sed -i "s/productName=\"TDengine\"/productName=\"${productName}\"/g" ${install_dir}/bin/remove.sh

        sed -i 's/verMode=edge/verMode=cluster/g' ${install_dir}/install_client.sh
        sed -i "s/serverName2=\"taosd\"/serverName2=\"${prefix}d\"/g" ${install_dir}/install_client.sh
        sed -i "s/clientName2=\"taos\"/clientName2=\"${prefix}\"/g" ${install_dir}/install_client.sh
        sed -i "s/configFile2=\"taos.cfg\"/configFile2=\"${prefix}.cfg\"/g" ${install_dir}/install_client.sh
        sed -i "s/productName2=\"TDengine\"/productName2=\"${productName}\"/g" ${install_dir}/install_client.sh
        sed -i "s/emailName2=\"taosdata.com\"/emailName2=\"${cusDomain}\"/g" ${install_dir}/install_client.sh

        sed -i 's/verMode=edge/verMode=cluster/g' ${install_dir}/bin/remove_client.sh
        sed -i "s/clientName2=\"taos\"/clientName2=\"${prefix}\"/g" ${install_dir}/bin/remove_client.sh
        sed -i "s/configFile2=\"taos\"/configFile2=\"${prefix}\"/g" ${install_dir}/bin/remove_client.sh
        sed -i "s/productName2=\"TDengine\"/productName2=\"${productName}\"/g" ${install_dir}/bin/remove_client.sh
    else
        sed -i 's/versionType=\"enterprise\"/versionType=\"community\"/g' ${install_dir}/start-all.sh
        sed -i 's/versionType=\"enterprise\"/versionType=\"community\"/g' ${install_dir}/stop-all.sh
    fi
}

function make_linux_pkg() {    
    # make server package
    if [[ "${packageType}" == "all" || "${packageType}" == "server" ]]; then
        cd ${install_dir}
        
        # copy bin files
        serverBin=(remove.sh ${prefix} ${prefix}d ${prefix}keeper TDinsight.sh set_core.sh ${prefix}adapter ${prefix}d-dump-cfg.gdb  tdengine-datasource.zip udfd startPre.sh  ${prefix}Benchmark ${prefix}dump ${prefix}-explorer tdengine-datasource.zip.md5 quick_deploy.sh)
        mkdir -p ${install_dir}/${serverPackageName}/bin
        cd ${install_dir}/bin
        for bin in "${serverBin[@]}"; do
            if [ -f "${bin}" ]; then
                cp ${bin} ${install_dir}/${serverPackageName}/bin
            else
                if [ "$bin" != "taos-explorer" ]; then
                    echo "can not find ${bin} in ${install_dir}/bin"
                    exit 1
                fi
            fi
        done
        
        # copy cfg files
        serverCfg=(${prefix}adapter.service  ${prefix}adapter.toml  ${prefix}.cfg  ${prefix}d.service  ${prefix}keeper.service ${prefix}keeper.toml ${prefix}-explorer.service explorer.toml)
        mkdir -p ${install_dir}/${serverPackageName}/cfg
        cd ${install_dir}/cfg
        for cfg in "${serverCfg[@]}"; do
            if [ -f "${cfg}" ]; then
                cp ${cfg} ${install_dir}/${serverPackageName}/cfg 
            else
                if [ "$cfg" != "explorer.toml" ] && [ "$cfg" != "${prefix}-explorer.service" ] ; then
                    echo "can not find ${cfg} in ${install_dir}/cfg"
                    exit 1
                fi
            fi
        done

        cd ${install_dir}
        cp -r inc/ ${install_dir}/${serverPackageName}
        cp -r init.d/ ${install_dir}/${serverPackageName}

        cd ${serverPackageName}
        tar -zcv -f package.tar.gz * --remove-files ||:

        cd ${install_dir}
        cp -r connector/ driver/ examples/ share/ ${serverPackageName}/ || :
        if [ "${versionType}" != "community" ]; then
            cp -r taosx/ ${serverPackageName}/
        fi
        cp start-all.sh stop-all.sh install.sh README.md ${serverPackageName}/
        tar -czv -f ${serverPackageName}-${os_type}-${os_arch}.tar.gz ${serverPackageName}/
        
        mkdir -p ${archiveDir}
        cp ${serverPackageName}-${os_type}-${os_arch}.tar.gz ${archiveDir}
    fi    

    # make client package
    if [[ "${packageType}" == "all" || "${packageType}" == "client" ]]; then
        # copy bin files
        serverBin=(get_client.sh remove_client.sh set_core.sh ${prefix} ${prefix}Benchmark ${prefix}dump)
        mkdir -p ${install_dir}/${clientPackageName}/bin
        cd ${install_dir}/bin
        for bin in "${serverBin[@]}"; do
            cp ${bin} ${install_dir}/${clientPackageName}/bin || :
        done
        
        # copy cfg files
        mkdir -p ${install_dir}/${clientPackageName}/cfg
        cd ${install_dir}/cfg        
        cp ${prefix}.cfg ${install_dir}/${clientPackageName}/cfg || :
        
        cd ${install_dir}
        cp -r inc/ ${install_dir}/${clientPackageName}

        cd ${clientPackageName}
        tar -zcv -f package.tar.gz * --remove-files ||:
        
        cd ${install_dir}
        cp -r connector/ driver/ examples/ ${clientPackageName}/
        cp install_client.sh ${clientPackageName}/
        tar -czv -f ${clientPackageName}-${os_type}-${os_arch}.tar.gz ${clientPackageName}/
        
        mkdir -p ${archiveDir}
        cp ${clientPackageName}-${os_type}-${os_arch}.tar.gz ${archiveDir}
    fi

    if [[ "${versionType}" == "community" && "${os_type}" == "Linux" && "${os_arch}" == "x64" ]]; then
        verMode="edge"
        mkdir -p ${archiveDir}
        
        # make Lite server package
        cd ${install_dir}
        liteServerPackageName="${productName}-server-${version}-lite"
        rm -rf ${install_dir}/${liteServerPackageName}
        mkdir -p ${install_dir}/${liteServerPackageName}/bin
        cp ${install_dir}/bin/remove.sh ${install_dir}/bin/startPre.sh ${install_dir}/bin/${prefix} ${install_dir}/bin/${prefix}d ${install_dir}/${liteServerPackageName}/bin/
        strip ${install_dir}/${liteServerPackageName}/bin/${prefix}d
        strip ${install_dir}/${liteServerPackageName}/bin/${prefix}

        mkdir -p ${install_dir}/${liteServerPackageName}/cfg
        cp ${install_dir}/cfg/${prefix}.cfg ${install_dir}/cfg/${prefix}d.service  ${install_dir}/${liteServerPackageName}/cfg/

        cp -r ${install_dir}/inc ${install_dir}/${liteServerPackageName}/
        cp -r ${install_dir}/init.d ${install_dir}/${liteServerPackageName}/

        cd ${liteServerPackageName}
        tar -zcv -f package.tar.gz * --remove-files ||:

        cd ${install_dir}
        cp -r ${install_dir}/driver ${install_dir}/${liteServerPackageName}/
        mkdir -p ${install_dir}/${liteServerPackageName}/examples
        cp -r ${install_dir}/examples/c ${install_dir}/${liteServerPackageName}/examples
        cp install.sh README.md ${liteServerPackageName}/
        sed -i 's/pagMode=full/pagMode=lite/g' ${liteServerPackageName}/install.sh
        tar -czv -f ${serverPackageName}-${os_type}-${os_arch}-Lite.tar.gz ${liteServerPackageName}/
        cp ${serverPackageName}-${os_type}-${os_arch}-Lite.tar.gz ${archiveDir}

        # make Lite client package
        cd ${install_dir}
        liteClientPackageName="${productName}-client-${version}-lite"
        rm -rf ${install_dir}/${liteClientPackageName}
        mkdir -p ${install_dir}/${liteClientPackageName}/bin
        cp ${install_dir}/bin/remove_client.sh ${install_dir}/bin/${prefix} ${install_dir}/${liteClientPackageName}/bin/
        strip ${install_dir}/${liteClientPackageName}/bin/${prefix}

        mkdir -p ${install_dir}/${liteClientPackageName}/cfg
        cp ${install_dir}/cfg/${prefix}.cfg  ${install_dir}/${liteClientPackageName}/cfg/

        cp -r ${install_dir}/inc ${install_dir}/${liteClientPackageName}/
        cd ${liteClientPackageName}
        tar -zcv -f package.tar.gz * --remove-files ||:

        cd ${install_dir}
        cp -r ${install_dir}/driver ${install_dir}/${liteClientPackageName}/
        mkdir -p ${install_dir}/${liteClientPackageName}/examples
        cp -r ${install_dir}/examples/c ${install_dir}/${liteClientPackageName}/examples
        cp install_client.sh ${install_dir}/${liteClientPackageName}/
        sed -i 's/pagMode=full/pagMode=lite/g' ${liteClientPackageName}/install_client.sh
        tar -czv -f ${clientPackageName}-${os_type}-${os_arch}-Lite.tar.gz ${liteClientPackageName}/
        cp ${clientPackageName}-${os_type}-${os_arch}-Lite.tar.gz ${archiveDir}

        # make deb package
        ret='0'
        command -v dpkg >/dev/null 2>&1 || { ret='1'; }

        if [ "$ret" -eq 0 ]; then
            output_dir="${communityDir}/debs"
            if [ -d ${output_dir} ]; then
                rm -rf ${output_dir}
            fi
            mkdir -p ${output_dir}
            
            cd ${communityDir}/packaging/deb
            ${csudo}./makedeb.sh ${debugDir} ${output_dir} ${version} ${os_arch} ${os_type} ${verMode} stable
            cp ${output_dir}/*.deb ${archiveDir}
        else
            echo "==========dpkg command not exist, so not release deb package!!!"
            exit 1
        fi


        # make rpm package
        ret='0'
        command -v rpmbuild >/dev/null 2>&1 || { ret='1'; }

        if [ "$ret" -eq 0 ]; then
            echo "====do rpm package for the centos system===="
            output_dir="${communityDir}/rpms"
            if [ -d ${output_dir} ]; then
                rm -rf ${output_dir}
            fi
            mkdir -p ${output_dir}

            cd ${communityDir}/packaging/rpm
            ${csudo}./makerpm.sh ${debugDir} ${output_dir} ${version} ${os_arch} ${os_type} ${verMode} stable
            cp ${output_dir}/*.rpm ${archiveDir}
        else
            echo "==========rpmbuild command not exist, so not release rpm package!!!"
            exit 1
        fi
    fi
}

function make_mac_pkg() {
    sudo rm -rf /opt/tdengine/*

    if [ -d "/usr/local/Cellar/tdengine/$version" ];then
        sudo cp -rf /usr/local/Cellar/tdengine/$version/ /opt/tdengine/
    elif [ -d "/opt/homebrew/Cellar/tdengine/$version" ];then
        sudo cp -rf /opt/homebrew/Cellar/tdengine/$version/ /opt/tdengine/
    else
        sudo cp -rf /usr/local/taos/ /opt/tdengine/
    fi

    sudo rm -rf /opt/tdengine/data
    sudo rm -rf /opt/tdengine/log
    sudo mkdir -p /opt/tdengine/service
    sudo cp $communityDir/packaging/tools/{logo.png,TDengine,com.taosdata.*} /opt/tdengine/service/

    sudo mkdir -p /opt/tdengine/examples/taosbenchmark-json
    sudo cp $communityDir/tools/taos-tools/example/* /opt/tdengine/examples/taosbenchmark-json

    if [ "${versionType}" == "community" ]; then        
                
        sudo cp -f ${keeperDir}/taoskeeper /opt/tdengine/bin/
        sudo cp -f ${keeperDir}/taoskeeper.service /opt/tdengine/cfg/
        sudo cp -f ${keeperDir}/config/taoskeeper.toml /opt/tdengine/cfg/
        
        sudo cp -f ${explorerDir}/target/release/taos-explorer /opt/tdengine/bin/
        sudo cp -f ${explorerDir}/target/taos-explorer.service /opt/tdengine/cfg/
        sudo cp -f ${explorerDir}/server/examples/explorer.toml /opt/tdengine/cfg/

        sudo cp -f ${internalDir}/enterprise/packaging/start-all.sh /opt/tdengine/bin
        sudo cp -f ${internalDir}/enterprise/packaging/stop-all.sh /opt/tdengine/bin

        sudo sed -i '' 's/versionType=\"enterprise\"/versionType=\"community\"/g' /opt/tdengine/bin/start-all.sh
        sudo sed -i '' 's/versionType=\"enterprise\"/versionType=\"community\"/g' /opt/tdengine/bin/stop-all.sh

        sudo chmod ugo+w /opt/tdengine/bin/remove.sh
        sudo chmod ugo+w /opt/tdengine/bin/start-all.sh
        sudo chmod ugo+w /opt/tdengine/bin/stop-all.sh
        
        cd $communityDir/packaging/tools
        git checkout -- $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s/TDengine-.*-macOS-.*\</TDengine-server-$version-macOS-$os_arch\</g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s/3.0.1.4/$version/g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|/opt.*/tools/post.sh|$communityDir/packaging/tools/post.sh|g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|/opt.*/tools/mac_before_install.txt|$communityDir/packaging/tools/mac_before_install.txt|g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|/opt/.*/release|$internalDir/release|g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|/opt.*/tools/mac_install_summary.txt|$communityDir/packaging/tools/mac_install_summary.txt|g" $communityDir/packaging/tools/TDengine.pkgproj

        /usr/local/bin/packagesbuild --package-version $version TDengine.pkgproj

        sudo rm -rf /opt/tdengine/{service,bin/taosd,bin/udfd,bin/taoskeeper,bin/taosadapter,bin/taos-explorer}
        sed -i '' "s/TDengine-.*-macOS-.*\</TDengine-client-$version-macOS-$os_arch\</g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s/mac_before_install.txt/mac_before_install_client.txt/g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|$communityDir/packaging/tools/mac_install_summary.txt|$communityDir/packaging/tools/mac_install_summary_client.txt|g" $communityDir/packaging/tools/TDengine.pkgproj
        /usr/local/bin/packagesbuild --package-version $version TDengine.pkgproj
    else
        sudo chmod ugo+w /opt/tdengine/bin/remove.sh
        
        cd $communityDir/packaging/tools
        git checkout -- $communityDir/packaging/tools/TDengine.pkgproj
        sudo rm -rf /opt/tdengine/{service,bin/taosd,bin/udfd,bin/taoskeeper,bin/taosadapter,bin/taos-explorer}

        sed -i '' "s/3.0.1.4/$version/g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|/opt.*/tools/post.sh|$communityDir/packaging/tools/post.sh|g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|/opt.*/tools/mac_before_install.txt|$communityDir/packaging/tools/mac_before_install_client.txt|g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s/TDengine-.*-macOS-.*\</TDengine-enterprise-client-$version-macOS-$os_arch\</g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|/opt/.*/release|$internalDir/release|g" $communityDir/packaging/tools/TDengine.pkgproj
        sed -i '' "s|/opt.*/tools/mac_install_summary.txt|$communityDir/packaging/tools/mac_install_summary_client.txt|g" $communityDir/packaging/tools/TDengine.pkgproj
        /usr/local/bin/packagesbuild --package-version $version TDengine.pkgproj
    fi
}

function verify_commit_id() {
    if ${commitCheck}; then
        binPath=$1
        binName=$2
        repoDir=$3
        
        index=2
        if [ "$binName" == "taosd" ] || [ "$binName" == "taosx" ] || [ "$binName" == "taos-explorer" ] ; then
            index=3
        fi
        
        if [ "$binName" == "taosd" ] && [ "$versionType" != "community" ]; then
            cd ${internalDir}
            expected_internal_commit_id=$(git rev-parse HEAD)
            cd ${communityDir}
            expected_commit_id=$(git rev-parse HEAD)

            actual_internal_commit_id=`${internalDir}/debug/build/bin/taosd -V | grep git | tail -n 1 | awk '{print $2}'`
            if [ "$actual_internal_commit_id" != "$expected_internal_commit_id" ]; then
                echo "commit id not match, expect $expected_internal_commit_id, actual $actual_internal_commit_id"
                exit 1
            fi

            actual_community_commit_id=`${internalDir}/debug/build/bin/taosd -V | grep git | head -n 1 | awk '{print $2}'`
            if [ "$actual_community_commit_id" != "$expected_commit_id" ]; then
                echo "commit id not match, expect $actual_community_commit_id, actual $expected_commit_id"
                exit 1
            fi

            actual_version=`${internalDir}/debug/build/bin/taosd -V | grep version | awk -v idx=$index '{print $idx}'`
            if [ "$actual_version" != "$version" ]; then
                echo "version not match, expect $version, actual $actual_version"
                exit 1
            fi
        else
            cd ${repoDir}        
            expected_commit_id=$(git rev-parse HEAD)
            
            actual_commit_id=`${binPath}/${binName} -V | grep git | awk '{print $2}'`
            length=${#actual_commit_id}
            expected_commit_id=${expected_commit_id:0:$length}
            if [ "$expected_commit_id" != "$actual_commit_id" ]; then
                echo "${binName} commit id not match, expect $expected_commit_id, actual $actual_commit_id"
                exit 1
            else
                echo "${binName} commit id match, expect $expected_commit_id, actual $actual_commit_id"
            fi

            actual_version=`${binPath}/${binName} -V | grep version | awk -v idx=$index '{print $idx}'`
            if [ "$actual_version" != "$version" ]; then
                echo "${binName} version not match, expect $version, actual $actual_version"
                exit 1
            fi

        fi
    fi
}

function makepkg() {
    if [ "${os_type}" == "Linux" ]; then
        make_linux_pkg
    elif [ "${os_type}" == "Darwin" ]; then
        make_mac_pkg
    fi
}

build_TDengine &
pid1=$!
build_taosx &
pid2=$!
build_taoskeeper &
pid3=$!
update_connectors &
pid4=$!

wait $pid1
if [ $? -ne 0 ]; then
    echo "build TDengine failed"
    exit 1
fi

wait $pid2
if [ $? -ne 0 ]; then
    echo "build taosx or taos-explorer failed"
    exit 1
fi

wait $pid3
if [ $? -ne 0 ]; then
    echo "build taoskeeper failed"
    exit 1
fi

wait $pid4
if [ $? -ne 0 ]; then
    echo "update connectors failed"
    exit 1
fi

if [ "${os_type}" != "Darwin" ]; then
    preparepkg
fi
makepkg

echo "Generate package done!"
