CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

FIND_PATH(HEADER_GTEST_INCLUDE_DIR gtest.h /usr/include/gtest /usr/local/include/gtest)
FIND_LIBRARY(LIB_GTEST_STATIC_DIR libgtest.a /usr/lib/ /usr/local/lib /usr/lib64)
FIND_LIBRARY(LIB_GTEST_SHARED_DIR libgtest.so /usr/lib/ /usr/local/lib /usr/lib64)

IF (HEADER_GTEST_INCLUDE_DIR AND (LIB_GTEST_STATIC_DIR OR LIB_GTEST_SHARED_DIR))
    MESSAGE(STATUS "gTest library found, build unit test")

    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)

    INCLUDE_DIRECTORIES(/usr/include /usr/local/include)
    LINK_DIRECTORIES(/usr/lib /usr/local/lib)

    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    ADD_EXECUTABLE(cliTest ${SOURCE_LIST})
    TARGET_LINK_LIBRARIES(cliTest taos tutil common gtest pthread)
ENDIF()
