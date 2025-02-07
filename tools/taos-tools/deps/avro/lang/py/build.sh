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

set -e

usage() {
  echo "Usage: $0 {clean|dist|interop-data-generate|interop-data-test|lint|test}"
  exit 1
}

clean() {
  git clean -xdf '*.avpr' \
                 '*.avsc' \
                 '*.egg-info' \
                 '*.py[co]' \
                 'VERSION.txt' \
                 '__pycache__' \
                 '.tox' \
                 'avro/test/interop' \
                 'dist' \
                 'userlogs'
}

dist() (
  ##
  # Use https://pypa-build.readthedocs.io to create the build artifacts.
  local destination virtualenv
  destination=$(
    d=../../dist/py
    mkdir -p "$d"
    cd -P "$d"
    pwd
  )
  virtualenv="$(mktemp -d)"
  python3 -m venv "$virtualenv"
  "$virtualenv/bin/python3" -m pip install build
  "$virtualenv/bin/python3" -m build --outdir "$destination"
)

interop-data-generate() {
  ./setup.py generate_interop_data
  cp -r avro/test/interop/data ../../build/interop
}

interop-data-test() {
  mkdir -p avro/test/interop ../../build/interop/data
  cp -r ../../build/interop/data avro/test/interop
  python3 -m unittest avro.test.test_datafile_interop
}

lint() {
  python3 -m tox -e lint
}

test_() {
  TOX_SKIP_ENV=lint python3 -m tox --skip-missing-interpreters
}

main() {
  (( $# )) || usage
  for target; do
    case "$target" in
      clean) clean;;
      dist) dist;;
      interop-data-generate) interop-data-generate;;
      interop-data-test) interop-data-test;;
      lint) lint;;
      test) test_;;
      *) usage;;
    esac
  done
}

main "$@"
