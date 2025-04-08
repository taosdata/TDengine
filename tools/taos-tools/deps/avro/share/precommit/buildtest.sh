#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRCMAKEIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_test_type buildtest

VERBOSE=false

# files that we want to kick off
BUILD_FILES=( build.sh )


buildtest_usage() {
  yetus_add_option "--verbose=<true|false>" "print output to console (default: false)"
}

# For now we only kick off the root build.sh
# buildtest_filefilter {
#   local filename=$1
#
#   if [[ ${filename} =~ build\.sh$ ]]; then
#     yetus_debug "Buildtest: run the tests for ${filename}"
#     add_test buildtest
#     yetus_add_array_element BUILD_FILES "${filename}"
#   fi
# }

buildtest_postcompile() {
  for file in "${BUILD_FILES[@]}"; do

    big_console_header "Running ${file}"

    #shellcheck disable=SC2001
    sanitized_filename=$(echo "${file}" | sed -e 's,[/\.],-,g')

    # Write both to stdout and the file using tee
    (cd ${BASEDIR} && ./${file} test) | tee -a ${PATCH_DIR}/build-${sanitized_filename}.txt
    result=${PIPESTATUS[0]}

    yetus_debug "Process exited with ${result}"

    if  (( result != 0 )); then
      add_vote_table -1 buildtest "The testsuite failed, please check the output"
      add_footer_table buildtest "@@BASE@@/build-${sanitized_filename}.txt"
      return 1
    fi

    add_vote_table +1 buildtest "The build has passed"
  done
}

buildtest_docker_support() {
  DOCKER_EXTRAARGS+=("--env" "JAVA=$JAVA")
}
