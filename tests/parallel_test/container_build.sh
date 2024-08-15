#!/bin/bash
set -e 

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -e enterprise edition"
    echo -e "\t -t make thread count"
    echo -e "\t -h help"
}

ent=0
while getopts "w:t:eh" opt; do
    case $opt in
        w)
            WORKDIR=$OPTARG
            ;;
        e)
            ent=1
            ;;
        t)
            THREAD_COUNT=$OPTARG
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
date 
docker run \
    -v $REP_MOUNT_PARAM \
    -v /root/.cargo/registry:/root/.cargo/registry \
    -v /root/.cargo/git:/root/.cargo/git \
    -v /root/go/pkg/mod:/root/go/pkg/mod \
    -v /root/.cache/go-build:/root/.cache/go-build \
    -v /root/.cos-local.1:/root/.cos-local.2 \
    -v ${REP_REAL_PATH}/enterprise/contrib/grant-lib:${REP_DIR}/enterprise/contrib/grant-lib \
    -v ${REP_REAL_PATH}/community/tools/taosadapter:${REP_DIR}/community/tools/taosadapter \
    -v ${REP_REAL_PATH}/community/tools/taos-tools:${REP_DIR}/community/tools/taos-tools \
    -v ${REP_REAL_PATH}/community/tools/taosws-rs:${REP_DIR}/community/tools/taosws-rs \
    -v ${REP_REAL_PATH}/community/contrib/apr/:${REP_DIR}/community/contrib/apr \
    -v ${REP_REAL_PATH}/community/contrib/apr-util/:${REP_DIR}/community/contrib/apr-util \
    -v ${REP_REAL_PATH}/community/contrib/cJson/:${REP_DIR}/community/contrib/cJson \
    -v ${REP_REAL_PATH}/community/contrib/cpp-stub/:${REP_DIR}/community/contrib/cpp-stub \
    -v ${REP_REAL_PATH}/community/contrib/curl/:${REP_DIR}/community/contrib/curl \
    -v ${REP_REAL_PATH}/community/contrib/curl2/:${REP_DIR}/community/contrib/curl2 \
    -v ${REP_REAL_PATH}/community/contrib/geos/:${REP_DIR}/community/contrib/geos \
    -v ${REP_REAL_PATH}/community/contrib/googletest/:${REP_DIR}/community/contrib/googletest \
    -v ${REP_REAL_PATH}/community/contrib/libs3/:${REP_DIR}/community/contrib/libs3 \
    -v ${REP_REAL_PATH}/community/contrib/libuv/:${REP_DIR}/community/contrib/libuv \
    -v ${REP_REAL_PATH}/community/contrib/lz4/:${REP_DIR}/community/contrib/lz4 \
    -v ${REP_REAL_PATH}/community/contrib/lzma2/:${REP_DIR}/community/contrib/lzma2 \
    -v ${REP_REAL_PATH}/community/contrib/mxml/:${REP_DIR}/community/contrib/mxml \
    -v ${REP_REAL_PATH}/community/contrib/openssl/:${REP_DIR}/community/contrib/openssl \
    -v ${REP_REAL_PATH}/community/contrib/pcre2/:${REP_DIR}/community/contrib/pcre2 \
    -v ${REP_REAL_PATH}/community/contrib/xml2/:${REP_DIR}/community/contrib/xml2 \
    -v ${REP_REAL_PATH}/community/contrib/zlib/:${REP_DIR}/community/contrib/zlib \
    -v ${REP_REAL_PATH}/community/contrib/zstd/:${REP_DIR}/community/contrib/zstd \
    --rm --ulimit core=-1 taos_test:v1.0 sh -c "pip uninstall taospy -y;pip3 install taospy==2.7.2;cd $REP_DIR;rm -rf debug;mkdir -p debug;cd debug;cmake .. -DBUILD_HTTP=false -DBUILD_TOOLS=true -DBUILD_TEST=true -DWEBSOCKET=true -DBUILD_TAOSX=false -DJEMALLOC_ENABLED=0 -DCMAKE_EXPORT_COMPILE_COMMANDS=1 ;make -j|| exit 1"
 # -v ${REP_REAL_PATH}/community/contrib/jemalloc/:${REP_DIR}/community/contrib/jemalloc \

if [[ -d ${WORKDIR}/debugNoSan  ]] ;then
    echo "delete  ${WORKDIR}/debugNoSan"
    rm -rf  ${WORKDIR}/debugNoSan
fi
if [[ -d ${WORKDIR}/debugSan ]] ;then
    echo "delete  ${WORKDIR}/debugSan"
    rm -rf  ${WORKDIR}/debugSan
fi
if [[ -d ${WORKDIR}/debugRelease ]] ;then
    echo "delete  ${WORKDIR}/debugRelease"
    rm -rf  ${WORKDIR}/debugRelease
fi

# if [ "$(uname -m)" = "aarch64" ] ;then
#     CMAKE_BUILD_TYPE="-DCMAKE_BUILD_TYPE=Debug"
# else
#     CMAKE_BUILD_TYPE="-DCMAKE_BUILD_TYPE=Release"
# fi

mv  ${REP_REAL_PATH}/debug  ${WORKDIR}/debugNoSan
date
docker run \
    -v $REP_MOUNT_PARAM \
    -v /root/.cargo/registry:/root/.cargo/registry \
    -v /root/.cargo/git:/root/.cargo/git \
    -v /root/go/pkg/mod:/root/go/pkg/mod \
    -v /root/.cache/go-build:/root/.cache/go-build \
    -v /root/.cos-local.1:/root/.cos-local.2 \
    -v ${REP_REAL_PATH}/enterprise/contrib/grant-lib:${REP_DIR}/enterprise/contrib/grant-lib \
    -v ${REP_REAL_PATH}/community/tools/taosadapter:${REP_DIR}/community/tools/taosadapter \
    -v ${REP_REAL_PATH}/community/tools/taos-tools:${REP_DIR}/community/tools/taos-tools \
    -v ${REP_REAL_PATH}/community/tools/taosws-rs:${REP_DIR}/community/tools/taosws-rs \
    -v ${REP_REAL_PATH}/community/tools/taosws-rs/target:${REP_DIR}/community/tools/taosws-rs/target \
    -v ${REP_REAL_PATH}/community/contrib/apr/:${REP_DIR}/community/contrib/apr \
    -v ${REP_REAL_PATH}/community/contrib/apr-util/:${REP_DIR}/community/contrib/apr-util \
    -v ${REP_REAL_PATH}/community/contrib/cJson/:${REP_DIR}/community/contrib/cJson \
    -v ${REP_REAL_PATH}/community/contrib/cpp-stub/:${REP_DIR}/community/contrib/cpp-stub \
    -v ${REP_REAL_PATH}/community/contrib/curl/:${REP_DIR}/community/contrib/curl \
    -v ${REP_REAL_PATH}/community/contrib/curl2/:${REP_DIR}/community/contrib/curl2 \
    -v ${REP_REAL_PATH}/community/contrib/geos/:${REP_DIR}/community/contrib/geos \
    -v ${REP_REAL_PATH}/community/contrib/googletest/:${REP_DIR}/community/contrib/googletest \
    -v ${REP_REAL_PATH}/community/contrib/libs3/:${REP_DIR}/community/contrib/libs3 \
    -v ${REP_REAL_PATH}/community/contrib/libuv/:${REP_DIR}/community/contrib/libuv \
    -v ${REP_REAL_PATH}/community/contrib/lz4/:${REP_DIR}/community/contrib/lz4 \
    -v ${REP_REAL_PATH}/community/contrib/lzma2/:${REP_DIR}/community/contrib/lzma2 \
    -v ${REP_REAL_PATH}/community/contrib/mxml/:${REP_DIR}/community/contrib/mxml \
    -v ${REP_REAL_PATH}/community/contrib/openssl/:${REP_DIR}/community/contrib/openssl \
    -v ${REP_REAL_PATH}/community/contrib/pcre2/:${REP_DIR}/community/contrib/pcre2 \
    -v ${REP_REAL_PATH}/community/contrib/xml2/:${REP_DIR}/community/contrib/xml2 \
    -v ${REP_REAL_PATH}/community/contrib/zlib/:${REP_DIR}/community/contrib/zlib \
    -v ${REP_REAL_PATH}/community/contrib/zstd/:${REP_DIR}/community/contrib/zstd \
    --rm --ulimit core=-1 taos_test:v1.0 sh -c "pip uninstall taospy -y;pip3 install taospy==2.7.2;cd $REP_DIR;rm -rf debug;mkdir -p debug;cd debug;cmake .. -DBUILD_HTTP=false -DBUILD_TOOLS=true -DBUILD_TEST=false -DWEBSOCKET=true   -DBUILD_SANITIZER=1  -DTOOLS_SANITIZE=true -DCMAKE_BUILD_TYPE=Debug -DTOOLS_BUILD_TYPE=Debug -DBUILD_TAOSX=false -DJEMALLOC_ENABLED=0;make -j|| exit 1 "

mv  ${REP_REAL_PATH}/debug  ${WORKDIR}/debugSan

date
# docker run \
#     -v $REP_MOUNT_PARAM \
#     -v /root/.cargo/registry:/root/.cargo/registry \
#     -v /root/.cargo/git:/root/.cargo/git \
#     -v /root/go/pkg/mod:/root/go/pkg/mod \
#     -v /root/.cache/go-build:/root/.cache/go-build \
#     -v /root/.cos-local.1:/root/.cos-local.2 \
#     -v ${REP_REAL_PATH}/enterprise/contrib/grant-lib:${REP_DIR}/enterprise/contrib/grant-lib \
#     -v ${REP_REAL_PATH}/community/tools/taosadapter:${REP_DIR}/community/tools/taosadapter \
#     -v ${REP_REAL_PATH}/community/tools/taos-tools:${REP_DIR}/community/tools/taos-tools \
#     -v ${REP_REAL_PATH}/community/tools/taosws-rs:${REP_DIR}/community/tools/taosws-rs \
#     -v ${REP_REAL_PATH}/community/tools/taosws-rs/target:${REP_DIR}/community/tools/taosws-rs/target \
#     -v ${REP_REAL_PATH}/community/contrib/apr/:${REP_DIR}/community/contrib/apr \
#     -v ${REP_REAL_PATH}/community/contrib/apr-util/:${REP_DIR}/community/contrib/apr-util \
#     -v ${REP_REAL_PATH}/community/contrib/cJson/:${REP_DIR}/community/contrib/cJson \
#     -v ${REP_REAL_PATH}/community/contrib/cpp-stub/:${REP_DIR}/community/contrib/cpp-stub \
#     -v ${REP_REAL_PATH}/community/contrib/curl/:${REP_DIR}/community/contrib/curl \
#     -v ${REP_REAL_PATH}/community/contrib/curl2/:${REP_DIR}/community/contrib/curl2 \
#     -v ${REP_REAL_PATH}/community/contrib/geos/:${REP_DIR}/community/contrib/geos \
#     -v ${REP_REAL_PATH}/community/contrib/googletest/:${REP_DIR}/community/contrib/googletest \
#     -v ${REP_REAL_PATH}/community/contrib/libs3/:${REP_DIR}/community/contrib/libs3 \
#     -v ${REP_REAL_PATH}/community/contrib/libuv/:${REP_DIR}/community/contrib/libuv \
#     -v ${REP_REAL_PATH}/community/contrib/lz4/:${REP_DIR}/community/contrib/lz4 \
#     -v ${REP_REAL_PATH}/community/contrib/lzma2/:${REP_DIR}/community/contrib/lzma2 \
#     -v ${REP_REAL_PATH}/community/contrib/mxml/:${REP_DIR}/community/contrib/mxml \
#     -v ${REP_REAL_PATH}/community/contrib/openssl/:${REP_DIR}/community/contrib/openssl \
#     -v ${REP_REAL_PATH}/community/contrib/pcre2/:${REP_DIR}/community/contrib/pcre2 \
#     -v ${REP_REAL_PATH}/community/contrib/xml2/:${REP_DIR}/community/contrib/xml2 \
#     -v ${REP_REAL_PATH}/community/contrib/zlib/:${REP_DIR}/community/contrib/zlib \
#     -v ${REP_REAL_PATH}/community/contrib/zstd/:${REP_DIR}/community/contrib/zstd \
#     --rm --ulimit core=-1 taos_test:v1.0 sh -c "pip uninstall taospy -y;pip3 install taospy==2.7.2;cd $REP_DIR;rm -rf debug;mkdir -p debug;cd debug;cmake .. -DBUILD_HTTP=false -DBUILD_TOOLS=true -DBUILD_TEST=false -DWEBSOCKET=true -DCMAKE_BUILD_TYPE=Release -DBUILD_TAOSX=false -DJEMALLOC_ENABLED=0;make -j || exit 1 "

# mv  ${REP_REAL_PATH}/debug  ${WORKDIR}/debugRelease



ret=$?
exit $ret

