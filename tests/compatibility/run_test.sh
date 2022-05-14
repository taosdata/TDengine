#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -o old package"
    echo -e "\t -n new package"
    echo -e "\t -c client package"
    echo -e "\t -h help"
}

while getopts "w:o:n:c:h" opt; do
    case $opt in
        w)
            WORK_DIR=$OPTARG
            ;;
        o)
            OLD_PACKAGE=$OPTARG
            ;;
        n)
            NEW_PACKAGE=$OPTARG
            ;;
        c)
            CLIENT_PACKAGE_PARAM="-c $OPTARG"
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
if [ -z "$OLD_PACKAGE" ]; then
    usage
    exit 1
fi
if [ -z "$NEW_PACKAGE" ]; then
    usage
    exit 1
fi

script_dir=`dirname $0`
cd $script_dir

pkg_name=`basename $NEW_PACKAGE`
new_version=`echo "$pkg_name"|sed "s/TDengine-enterprise-server-//"|sed "s/-Linux-x64.tar.gz//"`
pkg_name=`basename $OLD_PACKAGE`
old_version=`echo "$pkg_name"|sed "s/TDengine-enterprise-server-//"|sed "s/-Linux-x64.tar.gz//"`

source settings.sh

containers="$container_name1 $container_name2 $container_name3"

# initialize all containers and install with old version package
./init.sh -w $WORK_DIR -o $OLD_PACKAGE -n $NEW_PACKAGE $CLIENT_PACKAGE_PARAM

# upgrade with new version package
function upgrade() {
    local container_name=$1
    local new_pkg_name=`basename $NEW_PACKAGE`
    local new_pkg_dir=`echo "$new_pkg_name"|sed "s/-Linux-x64.tar.gz//"`
    local ret=0
    echo "upgrade ${container_name}"
    docker exec $container_name service taosd stop
    ret=$?
    if [ $ret -ne 0 ]; then
        echo "docker exec $container_name service taosd stop, exit: $ret"
        return $ret
    fi
    docker exec $container_name sh -c "cd /home/$new_pkg_dir;./install.sh -v server -e no"
    ret=$?
    if [ $ret -ne 0 ]; then
        echo "docker exec $container_name install.sh, exit: $ret"
        return $ret
    fi
    docker exec $container_name service taosd start
    ret=$?
    if [ $ret -ne 0 ]; then
        echo "docker exec $container_name service taosd start, exit: $ret"
        return $ret
    fi
    return 0
}

function checkStatus() {
    local container_name=$1
    local check_version=$2
    echo "python3 manualTest/TD-5114/checkClusterStatus.py $container_name $check_version"
    docker exec $container_name4 sh -c "cd /home/TDinternal/community/tests/pytest;python3 manualTest/TD-5114/checkClusterStatus.py $container_name $check_version"
    return $?
}

# config container /etc/taos/taos.cfg
taos_cfg=/etc/taos/taos.cfg
for container in $containers; do
    docker exec $container sed -i "s/^.*firstEp.*$/firstEp $container_name1:6030/" $taos_cfg
    docker exec $container sed -i "s/^.*fqdn.*$/fqdn $container/" $taos_cfg
    docker exec $container sed -i "s/^.*numOfMnodes.*$/numOfMnodes 3/" $taos_cfg
done

# start taosd
docker exec $container_name1 service taosd start
docker exec $container_name4 taos -h $container_name1 -s "CREATE DNODE \"$container_name2:6030\";"
docker exec $container_name4 taos -h $container_name1 -s "CREATE DNODE \"$container_name3:6030\";"

# start taosd
docker exec $container_name2 service taosd start
docker exec $container_name3 service taosd start

sleep 10

# show nodes
docker exec $container_name4 taos -h $container_name1 -s "SHOW DNODES;"
docker exec $container_name4 taos -h $container_name1 -s "SHOW MNODES;"

# check cluster status
for container in $containers; do
    checkStatus $container $old_version
    RET=$?
    if [ $RET -ne 0 ]; then
        echo "check cluster status $container error: $RET"
        exit $RET
    fi
    echo "check cluster status $container ret: $RET"
done

sleep 1

# upgrade
upgrade ${container_name3}
RET=$?
if [ $RET -ne 0 ]; then
    echo "upgrade ${container_name3} error: $RET"
    exit $RET
fi
sleep 10
# check cluster status
checkStatus ${container_name3} $old_version
RET=$?
if [ $RET -ne 0 ]; then
    echo "check cluster status ${container_name3} error: $RET"
    exit $RET
fi
echo "check cluster status ${container_name3} ret: $RET"

# upgrade
upgrade ${container_name2}
RET=$?
if [ $RET -ne 0 ]; then
    echo "upgrade ${container_name2} error: $RET"
    exit $RET
fi
sleep 10
# check cluster status
checkStatus ${container_name2} $old_version
RET=$?
if [ $RET -ne 0 ]; then
    echo "check cluster status ${container_name2} error: $RET"
    exit $RET
fi
echo "check cluster status ${container_name2} ret: $RET"

# upgrade
upgrade ${container_name1}
RET=$?
if [ $RET -ne 0 ]; then
    echo "upgrade ${container_name1} error: $RET"
    exit $RET
fi
sleep 10
# check cluster status
checkStatus ${container_name3} $new_version
RET=$?
if [ $RET -ne 0 ]; then
    echo "check cluster status ${container_name3} error: $RET"
    exit $RET
fi
echo "check cluster status ${container_name3} ret: $RET"

exit $RET

