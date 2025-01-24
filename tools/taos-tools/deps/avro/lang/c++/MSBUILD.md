<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Visual Studio 2019 Build Instructions

## Prerequisites

 * Microsoft Visual Studio 2019.
 * CMake >= 3.12 (should be supplied as part of VS2019 installation).
 * Clone [https://github.com/spektom/snappy-visual-cpp](https://github.com/spektom/snappy-visual-cpp), and follow build instructions in `README.md`.
 * Install Boost from [https://netcologne.dl.sourceforge.net/project/boost/boost-binaries/1.68.0/boost_1_68_0-msvc-14.1-64.exe](https://netcologne.dl.sourceforge.net/project/boost/boost-binaries/1.68.0/boost_1_68_0-msvc-14.1-64.exe).
 * Add `C:\<path to>\boost_1_68_0\lib64-msvc-14.1` to PATH environment variable.

## Building

    cd lang\c++
    cmake -G "Visual Studio 16 2019" -DBOOST_ROOT=C:\<path to>\boost_1_68_0 -DBOOST_INCLUDEDIR=c:\<path to>\boost_1_68_0\boost  -DBOOST_LIBRARYDIR=c:\<path to>\boost_1_68_0\lib64-msvc-14.1 -DSNAPPY_INCLUDE_DIR=C:\<path to>\snappy-visual-cpp -DSNAPPY_LIBRARIES=C:\<path to>\snappy-visual-cpp\x64\Release\snappy.lib ..
    msbuild Avro-cpp.sln /p:Configuration=Release /p:Platform=x64

