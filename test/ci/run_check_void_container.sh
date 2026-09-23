#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

function usage() {
    echo "$0"
    echo -e "\t -d work dir"
    echo -e "\t -h help"
}

while getopts "d:h" opt; do
    case $opt in
        d)
            WORKDIR=$OPTARG
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


 # enterprise edition
INTERNAL_REPDIR=$WORKDIR/TDinternal

REP_MOUNT_PARAM="$INTERNAL_REPDIR:/home/TDinternal"

CONTAINER_TESTDIR=/home/TDinternal/community

check_assert_scripts="$CONTAINER_TESTDIR/test/ci/check_void.sh" 
func_list_file="$CONTAINER_TESTDIR/test/ci/func.txt"
source_file_path="$CONTAINER_TESTDIR/source/"

ulimit -c unlimited
cat << EOF
docker run \
    -v $REP_MOUNT_PARAM \
    --rm --ulimit core=-1 ${DOCKER_IMAGE_NAME:-tdengine-ci:0.3}  "$check_assert_scripts" -c $func_list_file -f $source_file_path
EOF

docker run \
    -v "$REP_MOUNT_PARAM" \
    --rm --ulimit core=-1 ${DOCKER_IMAGE_NAME:-tdengine-ci:0.3}  $check_assert_scripts -c $func_list_file -f $source_file_path

ret=$?
exit $ret

