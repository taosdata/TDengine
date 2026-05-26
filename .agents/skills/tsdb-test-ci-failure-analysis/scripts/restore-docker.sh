#!/bin/bash
# set -x
JENKINS_LOG=jenkins.log
# pr_num=14228
# n=1
function usage() {
    echo "$0"
    echo -e "\t -p PR number"
    echo -e "\t -n build number"
    echo -e "\t -c container name"
    echo -e "\t -h help"
}
while getopts "p:n:c:h" opt; do
    case $opt in
        p)
            pr_num=$OPTARG
            ;;
        n)
            n=$OPTARG
            ;;
        c)
            container_name=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 1
            ;;
    esac
done
if [ -z "$container_name" ]; then
    echo "container name not specified"
    usage
    exit 1
fi
if [ -z "$pr_num" ]; then
    echo "PR number not specified"
    usage
    exit 1
fi
if [ -z "$n" ]; then
    echo "build number not specified"
    usage
    exit 1
fi
pr_num=`echo "$pr_num"|sed "s/PR-//"`
container_count=`docker ps -a -f name=$container_name|wc -l`
if [ $container_count -gt 1 ]; then
    docker ps -a -f name=$container_name
    echo "container $container_name exists"
    exit 1
fi
cd $(dirname $0)
# 22131:20220625-113105 NewTest/PR-14228:PR-14228:1:3.0
# 22270:20220625-121154 NewTest/PR-14221:PR-14221:2:3.0
ci_hosts="\
192.168.1.47 \
192.168.1.48 \
192.168.1.49 \
192.168.1.52 \
192.168.0.212 \
192.168.0.215 \
192.168.0.217 \
192.168.0.219 \
"
# Use tail -1 to pick the last (most recent) match when the same PR+build_num
# appears multiple times (e.g. re-runs). The old grep -A 1 approach broke when
# there were multiple matches: it returned 5 lines (2 groups + "--" separator)
# instead of the expected 2, leaving next_line unset and making sed fail with
# "unexpected `,'" (sed -n "LINE,p" is invalid syntax).
curr=`grep -n "^[0-9]\{8\}-[0-9]\{6\}" jenkins.log | grep "PR-${pr_num}:${n}:" | tail -1`
if [ -z "$curr" ]; then
    echo "PR-${pr_num}:${n} not found"
    for host in $ci_hosts; do
        ssh root@$host "sh -c \"grep -n \\\"^[0-9]\\\\\{8\\\\\}-[0-9]\\\\\{6\\\\\}\\\" /var/lib/jenkins/workspace/jenkins.log | grep \\\"PR-${pr_num}:${n}:\\\"\""
        if [ $? -eq 0 ]; then
            echo "this PR is possibly on host $host"
            break
        fi
    done
    exit 1
fi

# check if it is TDinternal CI
internal=0
commit_prefix=community
echo "$curr"|grep -q TDinternalCI
if [ $? -eq 0 ]; then
    internal=1
    commit_prefix=tdinternal
fi

curr_line=`echo "$curr"|cut -d: -f1`
# Find the first timestamp line after curr_line to bound the search range.
# If none exists (last entry in file), next_line is empty and we read to EOF.
next_line=`awk -v s="$curr_line" 'NR>s && /^[0-9]{8}-[0-9]{6}/ {print NR-1; exit}' $JENKINS_LOG`

if [ -n "$next_line" ]; then
    details=`sed -n "${curr_line},${next_line}p" $JENKINS_LOG`
else
    details=`tail -n +"$curr_line" $JENKINS_LOG`
fi
merge_line=`echo "$details"|grep -A 10 "$commit_prefix log merged: "|grep "Merge .* into"|head -n1`
if [ -z "$merge_line" ]; then
    echo "merge commit not found"
    exit 1
fi
echo "$merge_line"
branch=`echo "$merge_line"|awk '{print $2}'`
commit_id=`echo "$merge_line"|awk '{print $4}'`
# echo "$details"
community_id=`echo "$details"|grep "community log: commit"|awk '{print $NF}'`
internal_id=`echo "$details"|grep "tdinternal log: commit"|awk '{print $NF}'`
python_connector_id=`echo "$details"|grep "python connector log: commit"|awk '{print $NF}'`
# change_branch=`echo "$details"|grep "CHANGE_BRANCH"|sed "s/CHANGE_BRANCH://"`

# if [ -z "${branch}" ]; then
#     branch="$change_branch"
# fi

PWD=`pwd`
log_dir=`ls log|grep "PR-${pr_num}_${n}_"`
if [ -z "$log_dir" ]; then
    echo "no log dir found"
else
    mount_dir="-v ${PWD}/log/$log_dir:/home/log"
    local_ip=$(hostname -I | awk '{print $1}')
    build_dir=$(ls log/$log_dir | grep "build_${local_ip}")
    if [ -z "$build_dir" ]; then
        build_dir=$(ls log/$log_dir | grep "build_" | head -1)
    fi
    if [ ! -z "$build_dir" ]; then
        mount_dir="$mount_dir -v ${PWD}/log/$log_dir/$build_dir:/home/TDinternal/debug/build"
    fi
fi

docker run -d --privileged -it --name $container_name \
    $mount_dir \
    tdengine-ci:0.1 bash

if [ $internal -eq 0 ]; then
    docker exec $container_name /home/setup.sh -c $commit_id -m $branch -n
    echo "TDinternal checkout: $internal_id"
    docker exec $container_name sh -c "cd /home/TDinternal; git checkout $internal_id"
else
    docker exec $container_name /home/setup.sh -e -c $commit_id -m $branch -n
    echo "community checkout: $community_id"
    docker exec $container_name sh -c "cd /home/TDinternal/community; git checkout $community_id"
fi
echo
echo "* run the following command to enter the container:"
echo "         docker exec -it $container_name bash"
if [ -z "$log_dir" ]; then
    echo "* no log dir found"
else
    echo "* log and coredump files are located in /home/log"
fi
if [ -z "$build_dir" ]; then
    echo "* no build dir found"
else
    echo "* build files are located in /home/TDinternal/debug/build"
fi
echo "* source files are located in /home/TDinternal"
