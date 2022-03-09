#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -c community version"
    echo -e "\t -t make thread count"
    echo -e "\t -h help"
}

while getopts "w:t:ch" opt; do
    case $opt in
        w)
            WORKDIR=$OPTARG
            ;;
        c)
            COMMUNITY=community
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
if [ -z "$THREAD_COUNT" ]; then
    THREAD_COUNT=1
fi

ulimit -c unlimited

INTERNAL_REPDIR=$WORKDIR/TDinternal

docker run \
    -v $INTERNAL_REPDIR:/home \
    --rm --ulimit core=-1 taos_test:v1.0 sh -c "cd /home/$COMMUNITY;rm -rf debug;mkdir -p debug;cd debug;cmake .. -DBUILD_HTTP=false -DBUILD_TOOLS=true;make -j $THREAD_COUNT"

ret=$?
exit $ret
