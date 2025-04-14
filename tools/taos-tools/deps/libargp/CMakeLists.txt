cmake_minimum_required(VERSION 3.16)
project(libargp-mingw C)

set(CMAKE_C_STANDARD 99)

include_directories(src)
file(GLOB_RECURSE SRC_LIST ${PROJECT_SOURCE_DIR}/src/*.*)

add_executable(libargp-mingw ${SRC_LIST} argp-test.c)