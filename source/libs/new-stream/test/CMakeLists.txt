CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

MESSAGE(STATUS "build stream unit test")

include_directories("${TD_SOURCE_DIR}/include/libs/new-stream")
include_directories("${TD_SOURCE_DIR}/include/util")
include_directories("${TD_SOURCE_DIR}/include/common")
include_directories("${TD_SOURCE_DIR}/include/os")

ADD_EXECUTABLE(dataSinkTest dataSinkTest.cpp)
TARGET_LINK_LIBRARIES(dataSinkTest PRIVATE os util ${TAOS_NATIVE_LIB_STATIC}  common  new-stream gtest pthread)

add_test(
    NAME dataSinkTest
    COMMAND dataSinkTest
)
