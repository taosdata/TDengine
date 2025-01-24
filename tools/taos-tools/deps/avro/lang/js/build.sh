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

cd `dirname "$0"`

for target in "$@"
do
  case "$target" in
    lint)
      npm install
      npm run lint
      ;;
    test)
      npm install
      npm run cover
      ;;
    dist)
      npm pack
      mkdir -p ../../dist/js
      mv avro-js-*.tgz ../../dist/js
      ;;
    clean)
      rm -rf coverage
      ;;
    interop-data-generate)
      npm run interop-data-generate
      ;;
    interop-data-test)
      npm run interop-data-test
      ;;
    *)
      echo "Usage: $0 {lint|test|dist|clean}" >&2
      exit 1
  esac
done
