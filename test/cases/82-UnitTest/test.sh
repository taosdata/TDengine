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

# tdengine-ci ships CMake off PATH. The Linux unit-test job uses the builder
# image, where ctest is already on PATH.
if ! command -v ctest >/dev/null 2>&1; then
    for ctest_bin in /usr/local/cmake-*/bin/ctest /usr/local/bin/ctest; do
        if [ -x "$ctest_bin" ]; then
            export PATH="$(dirname "$ctest_bin"):${PATH}"
            break
        fi
    done
fi
if ! command -v ctest >/dev/null 2>&1; then
    echo "ERROR: ctest not found"
    exit 1
fi
if [ ! -x build/bin/taosd ]; then
    echo "ERROR: build/bin/taosd not found under ${PWD}"
    exit 1
fi
if [ ! -f CTestTestfile.cmake ] && [ ! -f community/CTestTestfile.cmake ]; then
    echo "ERROR: no CTestTestfile.cmake under ${PWD}"
    exit 1
fi

export PATH="${PWD}/build/bin:${PATH}"
export LD_LIBRARY_PATH="${PWD}/build/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
ulimit -n 1048576 2>/dev/null || ulimit -n 65536 2>/dev/null || true

# Same split as the Linux unit-test job: these tests own a service or run too
# long to share the parallel ctest, and are executed afterwards.
CTEST_EXCLUDE='timerTest|cunit_test|pcre*|example*|clientTest|connectOptionsTest|tmqTest|taoscTest|cosTest|tcs_test|osFileTests|idxFstTest|sessionTest|streamTriggerTaskTest'
CTEST_JOBS="${CTEST_PARALLEL:-8}"

pgrep taosd >/dev/null 2>&1 || build/bin/taosd >/dev/null 2>&1 &
sleep 10

ctest_log="unit-test.log"
echo "[unit-test] ctest -j${CTEST_JOBS} --timeout 900"
ctest -E "${CTEST_EXCLUDE}" --output-on-failure --timeout 900 -j"${CTEST_JOBS}" 2>&1 | tee "$ctest_log"
ctest_ret=${PIPESTATUS[0]}

if grep -q "No tests were found" "$ctest_log"; then
    echo "No tests were found, failing the script."
    exit 1
fi

overall=0
if [ "${ctest_ret}" -ne 0 ]; then
    echo "[FAIL] ctest exited ${ctest_ret}"
    overall=${ctest_ret}
fi

run_serial() {
    local name="$1"
    local bin="build/bin/${name}"
    local ret=0
    if [ ! -x "$bin" ]; then
        echo "[FAIL] missing ${bin}"
        overall=1
        return
    fi
    echo "[unit-test] ${name}"
    "$bin" >>"$ctest_log" 2>&1
    ret=$?
    if [ "${ret}" -ne 0 ]; then
        echo "[FAIL] ${name} exited ${ret}"
        overall=${ret}
    fi
}

run_serial clientTest
run_serial connectOptionsTest
run_serial sessionTest
run_serial timerTest
run_serial streamTriggerTaskTest

exit "${overall}"
