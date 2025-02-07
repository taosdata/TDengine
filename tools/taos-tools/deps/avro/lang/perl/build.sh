#!/bin/bash

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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e # exit on error

function usage {
  echo "Usage: $0 {lint|test|dist|clean|interop-data-generate|interop-data-test}"
  exit 1
}

if [ $# -eq 0 ]
then
  usage
fi

for target in "$@"
do

function do_clean(){
  [ ! -f Makefile ] || make clean
  rm -f  Avro-*.tar.gz META.yml Makefile.old
  rm -rf lang/perl/inc/
}

function do_lint(){
  local failures=0
  for i in $(find lib t xt -name '*.p[lm]' -or -name '*.t'); do
    if ! perlcritic --verbose 1 ${i}; then
      ((failures=failures+1))
    fi
  done
  if [ ${failures} -gt 0 ]; then
    return 1
  fi
}

case "$target" in
  lint)
    do_lint
    ;;

  test)
    perl ./Makefile.PL && make test
    ;;

  dist)
    cp ../../share/VERSION.txt .
    perl ./Makefile.PL && make dist
    ;;

  clean)
    do_clean
    ;;

  interop-data-generate)
    perl -Ilib share/interop-data-generate
    ;;

  interop-data-test)
    prove -Ilib xt/interop.t
    ;;

  *)
    usage
esac

done

exit 0
