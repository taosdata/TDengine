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
  echo "Usage: $0 {lint|test|dist|clean|install|doc|format}"
  exit 1
}

if [ $# -eq 0 ]
then
  usage
fi

if [ -f VERSION.txt ]
then
  VERSION=`cat VERSION.txt`
else
  VERSION=`cat ../../share/VERSION.txt`
fi

BUILD=../../build
AVRO_CPP=avro-cpp-$VERSION
AVRO_DOC=avro-doc-$VERSION
BUILD_DIR=../../build
BUILD_CPP=$BUILD/$AVRO_CPP
DIST_DIR=../../dist/$AVRO_CPP
DOC_CPP=$BUILD/$AVRO_DOC/api/cpp
DIST_DIR=../../dist/cpp
TARFILE=../dist/cpp/$AVRO_CPP.tar.gz

function do_doc() {
  doxygen
  if [ -d doc ]
  then
    mkdir -p $DOC_CPP
    cp -R doc/* $DOC_CPP
  else
    exit 1
  fi
}

function do_dist() {
  rm -rf $BUILD_CPP/
  mkdir -p $BUILD_CPP
  cp -r api AUTHORS build.sh CMakeLists.txt ChangeLog \
    LICENSE NOTICE impl jsonschemas NEWS parser README test examples \
    $BUILD_CPP
  find $BUILD_CPP -name '.svn' | xargs rm -rf
  cp ../../share/VERSION.txt $BUILD_CPP
  mkdir -p $DIST_DIR
  (cd $BUILD_DIR; tar cvzf $TARFILE $AVRO_CPP && cp $TARFILE $AVRO_CPP )
  if [ ! -f $DIST_FILE ]
  then
    exit 1
  fi
}

(mkdir -p build; cd build; cmake --version; cmake -G "Unix Makefiles" ..)
for target in "$@"
do

case "$target" in
  lint)
    # some versions of cppcheck seem to require an explicit
    # "--error-exitcode" option to return non-zero code
    cppcheck --error-exitcode=1 --inline-suppr -f -q -x c++ api examples impl test
    ;;

  test)
    (cd build && cmake -G "Unix Makefiles" -D CMAKE_BUILD_TYPE=Debug -D AVRO_ADD_PROTECTOR_FLAGS=1 .. && make && cd .. \
      && ./build/buffertest \
      && ./build/unittest \
      && ./build/CodecTests \
      && ./build/CompilerTests \
      && ./build/StreamTests \
      && ./build/SpecificTests \
      && ./build/AvrogencppTests \
      && ./build/DataFileTests   \
      && ./build/SchemaTests)
    ;;

  xcode-test)
    mkdir -p build.xcode
    (cd build.xcode \
        && cmake -G Xcode .. \
        && xcodebuild -configuration Release \
        && ctest -C Release)
    ;;

  dist)
    (cd build && cmake -G "Unix Makefiles" -D CMAKE_BUILD_TYPE=Release ..)
    do_dist
    do_doc
    ;;

  doc)
    do_doc
    ;;

  format)
    clang-format -i --style file `find api -type f` `find impl -type f` `find test -type f`
    ;;

  clean)
    (cd build && make clean)
    rm -rf doc test.avro test?.df test??.df test_skip.df test_lastSync.df test_readRecordUsingLastSync.df
    ;;

  install)
    (cd build && cmake -G "Unix Makefiles" -D CMAKE_BUILD_TYPE=Release .. && make install)
    ;;

  *)
    usage
esac

done

exit 0
