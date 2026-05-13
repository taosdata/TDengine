#!/bin/bash
set -x

function usage() {
    echo "$0"
    echo -e "\t -k container keyword"
    echo -e "\t -d destination directory"
    echo -e "\t -h help"
}

while getopts "k:d:h" opt; do
    case $opt in
        k)
            keyword=$OPTARG
            ;;
        d)
            dest_dir=$OPTARG
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
if [ -z "$keyword" ]; then
    echo "no keyword specified"
    usage
    exit 1
fi
if [ -z "$dest_dir" ]; then
    echo "no destination directory specified"
    usage
    exit 1
fi

containers=`docker ps --format "{{.Names}}"|grep "${keyword}"`
if [ -z "$containers" ]; then
    echo "no containers"
    exit 0
fi
backup_dirs="\
/var/log/taos \
"
core_dir=`cat /proc/sys/kernel/core_pattern`
echo "$core_dir"|grep -q "^/"
if [ $? -eq 0 ]; then
    core_dir=`dirname $core_dir`
    backup_dirs="$backup_dirs $core_dir"
fi
for container in $containers; do
    docker exec $container mkdir -p /backup
    for backup_dir in $backup_dirs; do
        backup_file=`basename $backup_dir`
        backup_file="${container}.${backup_file}.tar.gz"
        docker exec $container tar -czf /backup/${backup_file} $backup_dir
    done
    docker cp $container:/backup $dest_dir/
done

exit 0

