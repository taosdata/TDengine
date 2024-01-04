CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

FIND_PATH(HEADER_GTEST_INCLUDE_DIR gtest.h /usr/include/gtest /usr/local/include/gtest /usr/local/taos/include)
FIND_LIBRARY(LIB_GTEST_STATIC_DIR libgtest.a /usr/lib/ /usr/local/lib /usr/lib64 /usr/local/taos/driver/)
FIND_LIBRARY(LIB_GTEST_SHARED_DIR libgtest.so /usr/lib/ /usr/local/lib /usr/lib64 /usr/local/taos/driver/)

IF (HEADER_GTEST_INCLUDE_DIR AND (LIB_GTEST_STATIC_DIR OR LIB_GTEST_SHARED_DIR))
    MESSAGE(STATUS "gTest library found, build os test")

    INCLUDE_DIRECTORIES(${HEADER_GTEST_INCLUDE_DIR})
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ENDIF()

aux_source_directory(src OS_SRC)
# taoscTest
add_executable(taoscTest "taoscTest.cpp")
target_link_libraries(taoscTest taos os gtest_main)
target_include_directories(
    taoscTest
    PUBLIC "${TD_SOURCE_DIR}/include/os"
)
add_test(
    NAME taoscTest
    COMMAND taoscTest
)

