CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

# freemine: FIND_PATH(HEADER_GTEST_INCLUDE_DIR gtest.h /usr/include/gtest /usr/local/include/gtest)
# freemine: FIND_LIBRARY(LIB_GTEST_STATIC_DIR libgtest.a /usr/lib/ /usr/local/lib /usr/lib64)
# freemine: FIND_LIBRARY(LIB_GTEST_SHARED_DIR libgtest.so /usr/lib/ /usr/local/lib /usr/lib64)
# freemine: 
# freemine: IF(HEADER_GTEST_INCLUDE_DIR AND(LIB_GTEST_STATIC_DIR OR LIB_GTEST_SHARED_DIR))
# freemine:     MESSAGE(STATUS "gTest library found, build os test")
# freemine: 
# freemine:     INCLUDE_DIRECTORIES(${HEADER_GTEST_INCLUDE_DIR})
# freemine:     AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
# freemine: ENDIF()

INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/src/util/inc)

IF(TD_LINUX)
    ADD_EXECUTABLE(simTests "simTests.cpp")
    DEP_ext_gtest(simTests)
    TARGET_LINK_LIBRARIES(simTests PRIVATE os util tsim_static)

    ADD_TEST(
        NAME simTests
        COMMAND simTests
    )
ENDIF()
