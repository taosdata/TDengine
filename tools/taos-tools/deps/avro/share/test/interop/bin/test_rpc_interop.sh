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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

cd "${0%/*}/../../../.."

VERSION=$(<share/VERSION.txt)

export GEM_HOME="$PWD/lang/ruby/.gem/"

java_tool() {
  java -jar "lang/java/tools/target/avro-tools-$VERSION.jar" "$@"
}

py_tool() {
  PYTHONPATH=lang/py python3 -m avro.tool "$@"
}

ruby_tool() {
  ruby -Ilang/ruby/lib lang/ruby/test/tool.rb "$@"
}

tools=( {java,py,ruby}_tool )

proto=share/test/schemas/simple.avpr

portfile="/tmp/interop_$$"

cleanup() {
  rm -rf "$portfile"
  for job in $(jobs -p); do
    kill "$job" 2>/dev/null || true;
  done
}

trap 'cleanup' EXIT

for server in "${tools[@]}"; do
  for msgDir in share/test/interop/rpc/*; do
    msg="${msgDir##*/}"
    for c in "$msgDir/"*; do
      echo "TEST: $c"
      for client in "${tools[@]}"; do
        rm -rf "$portfile"
        "$server" rpcreceive 'http://127.0.0.1:0/' "$proto" "$msg" \
          -file "$c/response.avro" > "$portfile" &
        count=0
        until [[ -s "$portfile" ]]; do
          sleep 1
          if (( count++ >= 10 )); then
            echo "$server did not start." >&2
            exit 1
          fi
        done
        read -r _ port < "$portfile"
        "$client" rpcsend "http://127.0.0.1:$port" "$proto" "$msg" \
          -file "$c/request.avro"
      done
    done
  done
done
