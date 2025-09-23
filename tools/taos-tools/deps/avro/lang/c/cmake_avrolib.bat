REM  Licensed to the Apache Software Foundation (ASF) under one
REM  or more contributor license agreements.  See the NOTICE file
REM  distributed with this work for additional information
REM  regarding copyright ownership.  The ASF licenses this file
REM  to you under the Apache License, Version 2.0 (the
REM  "License"); you may not use this file except in compliance
REM  with the License.  You may obtain a copy of the License at
REM 
REM    https://www.apache.org/licenses/LICENSE-2.0
REM 
REM  Unless required by applicable law or agreed to in writing,
REM  software distributed under the License is distributed on an
REM  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
REM  KIND, either express or implied.  See the License for the
REM  specific language governing permissions and limitations
REM  under the License.

echo off

REM Set up the solution file in Windows. 

set my_cmake_path="put_your_cmake_path_here"
set cmake_path_win7="C:\Program Files (x86)\CMake 2.8\bin\cmake.exe"
set cmake_path_xp="C:\Program Files\CMake 2.8\bin\cmake.exe"

if exist %my_cmake_path% (
   set cmake_path=%my_cmake_path%
   goto RUN_CMAKE
)

if exist %cmake_path_win7% (
   set cmake_path=%cmake_path_win7%
   goto RUN_CMAKE
)

if exist %cmake_path_xp% (
   set cmake_path=%cmake_path_xp%
   goto RUN_CMAKE
)

echo "Set the proper cmake path in the variable 'my_cmake_path' in cmake_windows.bat, and re-run"
goto EXIT_ERROR

:RUN_CMAKE
%cmake_path% -G"Visual Studio 9 2008" -H. -Bbuild_win32


:EXIT_ERROR
