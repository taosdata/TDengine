#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e        # exit on error

root_dir=$(pwd)
build_dir="../../build/c"
dist_dir="../../dist/c"
version=$(./version.sh project)
tarball="avro-c-$version.tar.gz"
doc_dir="../../build/avro-doc-$version/api/c"

function prepare_build {
  clean
  mkdir -p $build_dir
  (cd $build_dir && cmake $root_dir -DCMAKE_BUILD_TYPE=RelWithDebInfo)
}

function clean {
  if [ -d $build_dir ]; then
    find $build_dir | xargs chmod 755
    rm -rf $build_dir
  fi
  rm -f VERSION.txt
  rm -f examples/quickstop.db
}

for target in "$@"
do

  case "$target" in

    interop-data-generate)
      prepare_build
      make -C $build_dir
      $build_dir/tests/generate_interop_data "../../share/test/schemas/interop.avsc"  "../../build/interop/data"
      ;;

    interop-data-test)
      prepare_build
      make -C $build_dir
      $build_dir/tests/test_interop_data "../../build/interop/data"
      ;;

    lint)
      echo 'This is a stub where someone can provide linting.'
      ;;

    test)
      prepare_build
      make -C $build_dir
      make -C $build_dir test
      ;;

    dist)
      prepare_build
      cp ../../share/VERSION.txt $root_dir
      make -C $build_dir docs
      # This is a hack to force the built documentation to be included
      # in the source package.
      cp $build_dir/docs/*.html $root_dir/docs
      make -C $build_dir package_source
      rm $root_dir/docs/*.html
      if [ ! -d $dist_dir ]; then
        mkdir -p $dist_dir
      fi
      if [ ! -d $doc_dir ]; then
        mkdir -p $doc_dir
      fi
      mv $build_dir/$tarball $dist_dir
      cp $build_dir/docs/*.html $doc_dir
      clean
      ;;

    clean)
      clean
      ;;

    *)
      echo "Usage: $0 {interop-data-generate|interop-data-test|lint|test|dist|clean}"
      exit 1
  esac

done

exit 0
