CMAKE_MINIMUM_REQUIRED(VERSION 3.0...3.20)
PROJECT(TDengine)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    SET(HOMEBREW_LIB_PATH "/opt/homebrew/lib")
    SET(HOMEBREW_INC_PATH "/opt/homebrew/include/gtest")
ENDIF ()

FIND_PATH(HEADER_GTEST_INCLUDE_DIR gtest.h /usr/include/gtest /usr/local/include/gtest ${HOMEBREW_INC_PATH})
FIND_LIBRARY(LIB_GTEST_STATIC_DIR libgtest.a /usr/lib/ /usr/local/lib /usr/lib64 ${HOMEBREW_LIB_PATH})
FIND_LIBRARY(LIB_GTEST_SHARED_DIR libgtest.so /usr/lib/ /usr/local/lib /usr/lib64 ${HOMEBREW_LIB_PATH})

IF (HEADER_GTEST_INCLUDE_DIR AND (LIB_GTEST_STATIC_DIR OR LIB_GTEST_SHARED_DIR))
    MESSAGE(STATUS "gTest library found, build unit test")

    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)

    get_filename_component(HEADER_GTEST_PATH ${HEADER_GTEST_INCLUDE_DIR} PATH)
    INCLUDE_DIRECTORIES(/usr/include /usr/local/include ${HEADER_GTEST_PATH})

    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    IF (LIB_GTEST_STATIC_DIR)
        get_filename_component(GTEST_LIB_PATH ${LIB_GTEST_STATIC_DIR} PATH)
        MESSAGE(STATUS "${Green} found libtest.a in ${GTEST_LIB_PATH}, will build cliTest ${ColourReset}")
        LINK_DIRECTORIES(/usr/lib /usr/local/lib ${GTEST_LIB_PATH})
        ADD_EXECUTABLE(cliTest ${SOURCE_LIST})
        TARGET_LINK_LIBRARIES(cliTest taos cJson tutil common gtest pthread)
    ENDIF()
ENDIF()
