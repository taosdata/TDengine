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
    --rm --ulimit core=-1 taos_test:v1.0 sh -c "pip uninstall taospy -y;pip3 install taospy==2.7.2;cd $REP_DIR;rm -rf debug;mkdir -p debug;cd debug;cmake .. -DBUILD_HTTP=false -DBUILD_TOOLS=true -DBUILD_TEST=true -DWEBSOCKET=true   -DBUILD_SANITIZER=1  -DTOOLS_SANITIZE=true $CMAKE_BUILD_TYPE -DTOOLS_BUILD_TYPE=Debug -DBUILD_TAOSX=false -DJEMALLOC_ENABLED=0;make -j 10|| exit 1 "
 # -v ${REP_REAL_PATH}/community/contrib/jemalloc/:${REP_DIR}/community/contrib/jemalloc \

if [[ -d ${WORKDIR}/debugNoSan  ]] ;then
    echo "delete  ${WORKDIR}/debugNoSan"
    rm -rf  ${WORKDIR}/debugNoSan
fi
if [[ -d ${WORKDIR}/debugSan ]] ;then
    echo "delete  ${WORKDIR}/debugSan"
    rm -rf  ${WORKDIR}/debugSan
fi

if [ "$(uname -m)" = "aarch64" ] ;then
    CMAKE_BUILD_TYPE="-DCMAKE_BUILD_TYPE=Debug"
else
    CMAKE_BUILD_TYPE="-DCMAKE_BUILD_TYPE=Release"
fi


rm -rf  ${REP_REAL_PATH}/debug 
date

ret=$?
exit $ret

