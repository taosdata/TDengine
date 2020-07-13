# generate debug version:
# mkdir debug; cd debug; cmake -DCMAKE_BUILD_TYPE=Debug ..
# generate release version:
# mkdir release; cd release; cmake -DCMAKE_BUILD_TYPE=Release ..

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

SET(CMAKE_C_STANDARD 11)
SET(CMAKE_VERBOSE_MAKEFILE ON)

ADD_SUBDIRECTORY(examples/c)
ADD_SUBDIRECTORY(tsim)
ADD_SUBDIRECTORY(test/c)
ADD_SUBDIRECTORY(comparisonTest/tdengine)
