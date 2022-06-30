#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -d TDinternal code dir"
    echo -e "\t -p old package"
    echo -e "\t -q new package"
    echo -e "\t -c container name"
    echo -e "\t -h help"
}

while getopts "w:p:q:n:c:d:h" opt; do
    case $opt in
        w)
            WORK_DIR=$OPTARG
            ;;
        d)
            CODE_DIR=$OPTARG
            ;;
        p)
            TAOS_PKG1=$OPTARG
            ;;
        q)
            TAOS_PKG2=$OPTARG
            ;;
        c)
            CONTAINER_NAME=$OPTARG
            ;;
        n)
            NET_NAME=$OPTARG
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

if [ -z "$WORK_DIR" ]; then
    usage
    exit 1
fi
if [ -z "$TAOS_PKG1" ]; then
    usage
    exit 1
fi
if [ -z "$TAOS_PKG2" ]; then
    usage
    exit 1
fi
if [ -z "$CONTAINER_NAME" ]; then
    usage
    exit 1
fi
if [ -z "$NET_NAME" ]; then
    usage
    exit 1
fi
if [ -z "$CODE_DIR" ]; then
    usage
    exit 1
fi
if [ ! -f "$TAOS_PKG1" ]; then
    echo "$TAOS_PKG1 not found"
    exit 1
fi
if [ ! -f "$TAOS_PKG2" ]; then
    echo "$TAOS_PKG2 not found"
    exit 1
fi

pkg_name1=`basename $TAOS_PKG1`
pkg_dir1=`echo "$pkg_name1"|sed "s/-Linux-x64.tar.gz//"`
pkg_name2=`basename $TAOS_PKG2`
pkg_dir2=`echo "$pkg_name2"|sed "s/-Linux-x64.tar.gz//"`

RET=0
docker run -d --name $CONTAINER_NAME \
    --hostname $CONTAINER_NAME \
    --net $NET_NAME --ulimit core=-1 -it \
    -v $TAOS_PKG1:/home/tdengine1.tar.gz:ro \
    -v $TAOS_PKG2:/home/tdengine2.tar.gz:ro \
    -v $WORK_DIR/coredump:/home/coredump \
    -v $CODE_DIR:/home/TDinternal \
    taos_test:v1.0 bash
RET=$?
if [ $RET -ne 0 ]; then
    echo "docker run failed with $RET"
    exit $RET
fi

docker exec $CONTAINER_NAME sh -c "cd /home;tar xzf tdengine1.tar.gz;tar xzf tdengine2.tar.gz;cd $pkg_dir1;./install.sh -v server -e no"
RET=$?
if [ $RET -ne 0 ]; then
    echo "docker exec install.sh failed with $RET"
    exit $RET
fi
exit 0

