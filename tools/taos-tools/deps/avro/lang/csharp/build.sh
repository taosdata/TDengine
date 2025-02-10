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

set -e                # exit on error
set -x

cd `dirname "$0"`                  # connect to root

ROOT=../..
VERSION=`cat $ROOT/share/VERSION.txt`

for target in "$@"
do

  case "$target" in

    lint)
      echo 'This is a stub where someone can provide linting.'
      ;;

    test)
      dotnet build --configuration Release Avro.sln

      # AVRO-2442: Explictly set LANG to work around ICU bug in `dotnet test`
      LANG=en_US.UTF-8 dotnet test  --configuration Release --no-build \
          --filter "TestCategory!=Interop" Avro.sln
      ;;

    perf)
      pushd ./src/apache/perf/
      dotnet run --configuration Release --framework net5.0
      ;;

    dist)
      # pack NuGet packages
      dotnet pack --configuration Release Avro.sln

      # add the binary LICENSE and NOTICE to the tarball
      mkdir build/
      cp LICENSE NOTICE build/

      # add binaries to the tarball
      mkdir build/main/
      cp -R src/apache/main/bin/Release/* build/main/
      mkdir build/codegen/
      cp -R src/apache/codegen/bin/Release/* build/codegen/

      # build the tarball
      mkdir -p ${ROOT}/dist/csharp
      (cd build; tar czf ${ROOT}/../dist/csharp/avro-csharp-${VERSION}.tar.gz main codegen LICENSE NOTICE)

      # build documentation
      doxygen Avro.dox
      mkdir -p ${ROOT}/build/avro-doc-${VERSION}/api/csharp
      cp -pr build/doc/* ${ROOT}/build/avro-doc-${VERSION}/api/csharp
      ;;

    interop-data-generate)
      dotnet run --project src/apache/test/Avro.test.csproj --framework net5.0 ../../share/test/schemas/interop.avsc ../../build/interop/data
      ;;

    interop-data-test)
      LANG=en_US.UTF-8 dotnet test --filter "TestCategory=Interop" --verbosity normal
      ;;

    clean)
      rm -rf src/apache/{main,test,codegen,ipc,msbuild,perf}/{obj,bin}
      rm -rf build
      rm -f  TestResult.xml
      ;;

    *)
      echo "Usage: $0 {lint|test|clean|dist|perf|interop-data-generate|interop-data-test}"
      exit 1

  esac

done
