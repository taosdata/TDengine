#!/bin/bash
set -e 

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -e enterprise edition"
    echo -e "\t -b branch of taosadapter, default is main"
    echo -e "\t -h help"
}

ent=0
branch_taosadapter=main
build_taosadapter=true
build_no_asan=true

while getopts "w:b:ehT:A:" opt; do
    case $opt in
        w)
            WORKDIR=$OPTARG
            ;;
        e)
            ent=1
            ;;
        b)  
            branch_taosadapter=$OPTARG
            ;;
        T)
            build_taosadapter=$OPTARG
            ;;
        A)
            build_no_asan=$OPTARG
            ;;        
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 0
            ;;
    esac
done

if [ -z "$WORKDIR" ]; then
    usage
    exit 1
fi
# if [ -z "$THREAD_COUNT" ]; then
#     THREAD_COUNT=1
# fi

ulimit -c unlimited

# ── 编译签名密钥：从固定路径加载，不纳入 Git ────────────────────────────────
# 注：环境变量由容器内 source 注入（Docker 不继承宿主机环境变量）。
# 此处在宿主机提前做两项检查：① 文件存在；② source 后变量非空。
# 这样编译报错能在进入容器之前就被捕获，定位更清晰。
SALT_ENV_FILE="/security/salt_env.sh"
if [ ! -f "$SALT_ENV_FILE" ]; then
    echo "ERROR: signature salt file not found: $SALT_ENV_FILE"
    echo "       Please create it on this build machine with:"
    echo "       export TD_ENTERPRISE_EDITION_SIGNATURE_SALT=<your-salt>"
    exit 1
fi
# shellcheck source=/dev/null
source "$SALT_ENV_FILE"
if [ -z "$TD_ENTERPRISE_EDITION_SIGNATURE_SALT" ]; then
    echo "ERROR: TD_ENTERPRISE_EDITION_SIGNATURE_SALT is empty after sourcing $SALT_ENV_FILE"
    echo "       Make sure the file contains: export TD_ENTERPRISE_EDITION_SIGNATURE_SALT=<your-salt>"
    exit 1
fi
echo "Signature salt loaded from $SALT_ENV_FILE (TD_ENTERPRISE_EDITION_SIGNATURE_SALT is set)"

if [ $ent -eq 0 ]; then
    REP_DIR=/home/TDengine
    REP_REAL_PATH=$WORKDIR/TDengine
    REP_MOUNT_PARAM=$REP_REAL_PATH:/home/TDengine
else
    REP_DIR=/home/TDinternal
    REP_REAL_PATH=$WORKDIR/TDinternal
    REP_MOUNT_PARAM=$REP_REAL_PATH:/home/TDinternal
    
fi

if [ "$build_taosadapter" = "true" ]; then
    BUILD_HTTP_OPT="-DBUILD_HTTP=tdinternal -DTAOSADAPTER_GIT_TAG:STRING=${branch_taosadapter}"
else
    BUILD_HTTP_OPT="-DBUILD_HTTP=true"
fi

date 

if [[ -d ${WORKDIR}/debugNoSan  ]] ;then
    echo "delete  ${WORKDIR}/debugNoSan"
    rm -rf  ${WORKDIR}/debugNoSan
fi
if [[ -d ${WORKDIR}/debugSan ]] ;then
    echo "delete  ${WORKDIR}/debugSan"
    rm -rf  ${WORKDIR}/debugSan
fi

if [ "$build_no_asan" = "true" ]; then
    docker run \
        -v $REP_MOUNT_PARAM \
        -v /root/.cargo/registry:/root/.cargo/registry \
        -v /root/.cargo/git:/root/.cargo/git \
        -v /root/go/pkg/mod:/root/go/pkg/mod \
        -v /root/.cache/go-build:/root/.cache/go-build \
        -v /root/.cos-local.1:/root/.cos-local.2 \
        -v ${REP_REAL_PATH}/enterprise/contrib/grant-lib:${REP_DIR}/enterprise/contrib/grant-lib \
        -v ${REP_REAL_PATH}/community/tools/taosadapter:${REP_DIR}/community/tools/taosadapter \
        -v ${REP_REAL_PATH}/community/tools/taosws-rs:${REP_DIR}/community/tools/taosws-rs \
        -v ${REP_REAL_PATH}/community/tools/taosws-rs/target:${REP_DIR}/community/tools/taosws-rs/target \
        -v /security:/security:ro \
        --rm --ulimit core=-1 tdengine-ci:0.1 sh -c ". /security/salt_env.sh || exit 1;cd $REP_DIR;apt update -y && apt install groff -y;rm -rf debug;mkdir -p debug;cd debug;cmake .. $BUILD_HTTP_OPT -DCOVER=true -DBUILD_TOOLS=true -DBUILD_TEST=true -DWEBSOCKET=true -DBUILD_TAOSX=false -DJEMALLOC_ENABLED=0 -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -DBUILD_CONTRIB=false ;make -j|| exit 1"
    # -v ${REP_REAL_PATH}/community/contrib/jemalloc/:${REP_DIR}/community/contrib/jemalloc \


    # if [ "$(uname -m)" = "aarch64" ] ;then
    #     CMAKE_BUILD_TYPE="-DCMAKE_BUILD_TYPE=Debug"
    # else
    #     CMAKE_BUILD_TYPE="-DCMAKE_BUILD_TYPE=Release"
    # fi

    mv  ${REP_REAL_PATH}/debug  ${WORKDIR}/debugNoSan
    date
fi

docker run \
    -v $REP_MOUNT_PARAM \
    -v /root/.cargo/registry:/root/.cargo/registry \
    -v /root/.cargo/git:/root/.cargo/git \
    -v /root/go/pkg/mod:/root/go/pkg/mod \
    -v /root/.cache/go-build:/root/.cache/go-build \
    -v /root/.cos-local.1:/root/.cos-local.2 \
    -v ${REP_REAL_PATH}/enterprise/contrib/grant-lib:${REP_DIR}/enterprise/contrib/grant-lib \
    -v ${REP_REAL_PATH}/community/tools/taosadapter:${REP_DIR}/community/tools/taosadapter \
    -v ${REP_REAL_PATH}/community/tools/taosws-rs:${REP_DIR}/community/tools/taosws-rs \
    -v ${REP_REAL_PATH}/community/tools/taosws-rs/target:${REP_DIR}/community/tools/taosws-rs/target \
    -v /security:/security:ro \
    --rm --ulimit core=-1 tdengine-ci:0.1 sh -c ". /security/salt_env.sh || exit 1;cd $REP_DIR;apt update -y && apt install groff -y;rm -rf debug;mkdir -p debug;cd debug;cmake ..  $BUILD_HTTP_OPT -DCOVER=true -DBUILD_TOOLS=true -DBUILD_TEST=true -DWEBSOCKET=true   -DBUILD_SANITIZER=1  -DTOOLS_SANITIZE=true -DCMAKE_BUILD_TYPE=Debug -DTOOLS_BUILD_TYPE=Debug -DBUILD_TAOSX=false -DJEMALLOC_ENABLED=0 -DBUILD_CONTRIB=false;make -j|| exit 1 "

mv  ${REP_REAL_PATH}/debug  ${WORKDIR}/debugSan
date


ret=$?
exit $ret
