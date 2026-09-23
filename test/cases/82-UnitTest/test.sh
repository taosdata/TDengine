#!/bin/bash

# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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
    echo -e "\t -e enterprise edition"
    echo -e "\t -h help"
}

ent=1
while getopts "e:h" opt; do
    case $opt in
        e)
            ent="$OPTARG"
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

script_dir=$(dirname "$0")
cd "${script_dir}"
PWD=$(pwd)

if [ $ent -eq 0 ]; then
    cd ../../../debug
else
    cd ../../../../debug
fi
PWD=$(pwd)
echo "PWD: $PWD"
set -e

pgrep taosd || build/bin/taosd >> /dev/null 2>&1 &

sleep 10

# Run ctest and capture output and return code
ctest_output="unit-test.log"
ctest -E "timerTest|cunit_test|pcre*|example*|clientTest|connectOptionsTest|tmqTest|taoscTest" -j8 2>&1 | tee "$ctest_output"
ctest_ret=${PIPESTATUS[0]}


# Read the captured output
ctest_output=$(cat "$ctest_output")

# Check if ctest failed or no tests were found
if [ $ctest_ret -ne 0 ]; then
    echo "ctest failed, failing the script."
    exit 1
elif echo "$ctest_output" | grep -q "No tests were found"; then
    echo "No tests were found, failing the script."
    exit 1
fi

build/bin/clientTest
build/bin/connectOptionsTest 
build/bin/instanceTest
build/bin/taoscTest
build/bin/timerTest
