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
        --rm --ulimit core=-1 tdengine-ci:0.1 sh -c "cd $REP_DIR;rm -rf debug;mkdir -p debug;cd debug;cmake .. $BUILD_HTTP_OPT -DBUILD_TOOLS=true -DBUILD_TEST=true -DWEBSOCKET=true -DBUILD_TAOSX=false -DJEMALLOC_ENABLED=0 -DCMAKE_EXPORT_COMPILE_COMMANDS=1 ;make -j|| exit 1"
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
    --rm --ulimit core=-1 tdengine-ci:0.1 sh -c "cd $REP_DIR;rm -rf debug;mkdir -p debug;cd debug;cmake ..  $BUILD_HTTP_OPT -DBUILD_TOOLS=true -DBUILD_TEST=true -DWEBSOCKET=true   -DBUILD_SANITIZER=1  -DTOOLS_SANITIZE=true -DCMAKE_BUILD_TYPE=Debug -DTOOLS_BUILD_TYPE=Debug -DBUILD_TAOSX=false -DJEMALLOC_ENABLED=0 ;make -j|| exit 1 "

mv  ${REP_REAL_PATH}/debug  ${WORKDIR}/debugSan
# if [ "$build_no_asan" = "true" ]; then
#     cp -f ${WORKDIR}/debugNoSan/build/bin/tmq_sim ${WORKDIR}/debugSan/build/bin/tmq_sim
#     cp -f ${WORKDIR}/debugNoSan/build/bin/sml_test ${WORKDIR}/debugSan/build/bin/sml_test
#     cp -f ${WORKDIR}/debugNoSan/build/lib/libudf* ${WORKDIR}/debugSan/build/lib/
# fi
date


ret=$?
exit $ret
