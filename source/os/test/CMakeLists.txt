CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

FIND_PATH(HEADER_GTEST_INCLUDE_DIR gtest.h /usr/include/gtest /usr/local/include/gtest)
FIND_LIBRARY(LIB_GTEST_STATIC_DIR libgtest.a /usr/lib/ /usr/local/lib /usr/lib64)
FIND_LIBRARY(LIB_GTEST_SHARED_DIR libgtest.so /usr/lib/ /usr/local/lib /usr/lib64)

IF(HEADER_GTEST_INCLUDE_DIR AND(LIB_GTEST_STATIC_DIR OR LIB_GTEST_SHARED_DIR))
    MESSAGE(STATUS "gTest library found, build os test")

    INCLUDE_DIRECTORIES(${HEADER_GTEST_INCLUDE_DIR})
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
ENDIF()

INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/src/util/inc)

if(TD_LINUX)
    add_executable(osAtomicTests "osAtomicTests.cpp")
    DEP_ext_gtest(osAtomicTests)
    target_link_libraries(osAtomicTests PRIVATE os util)
    add_test(
        NAME osAtomicTests
        COMMAND osAtomicTests
    )

    add_executable(osDirTests "osDirTests.cpp")
    DEP_ext_gtest(osDirTests)
    target_link_libraries(osDirTests PRIVATE os util)
    add_test(
        NAME osDirTests
        COMMAND osDirTests
    )

    add_executable(osFileTests "osFileTests.cpp")
    DEP_ext_gtest(osFileTests)
    target_link_libraries(osFileTests PRIVATE os util)
    add_test(
        NAME osFileTests
        COMMAND osFileTests
    )

    add_executable(osEnvTests "osEnvTests.cpp")
    DEP_ext_gtest(osEnvTests)
    target_link_libraries(osEnvTests PRIVATE os util)
    add_test(
        NAME osEnvTests
        COMMAND osEnvTests
    )

    add_executable(osTests "osTests.cpp")
    DEP_ext_gtest(osTests)
    target_link_libraries(osTests PRIVATE os util)
    add_test(
        NAME osTests
        COMMAND osTests
    )

    add_executable(osThreadTests "osThreadTests.cpp")
    DEP_ext_gtest(osThreadTests)
    target_link_libraries(osThreadTests PRIVATE os util)
    add_test(
        NAME osThreadTests
        COMMAND osThreadTests
    )
endif()

add_executable(osMathTests "osMathTests.cpp")
DEP_ext_gtest(osMathTests)
target_link_libraries(osMathTests PRIVATE os util)
add_test(
    NAME osMathTests
    COMMAND osMathTests
)

add_executable(osSemaphoreTests "osSemaphoreTests.cpp")
DEP_ext_gtest(osSemaphoreTests)
target_link_libraries(osSemaphoreTests PRIVATE os util)
add_test(
    NAME osSemaphoreTests
    COMMAND osSemaphoreTests
)

add_executable(osSleepTests "osSleepTests.cpp")
DEP_ext_gtest(osSleepTests)
target_link_libraries(osSleepTests PRIVATE os util)
add_test(
    NAME osSleepTests
    COMMAND osSleepTests
)

add_executable(osStringTests "osStringTests.cpp")
DEP_ext_gtest(osStringTests )
target_link_libraries(osStringTests PRIVATE os util)
add_test(
    NAME osStringTests
    COMMAND osStringTests
)

add_executable(osSystemTests "osSystemTests.cpp")
DEP_ext_gtest(osSystemTests)
target_link_libraries(osSystemTests PRIVATE os util)
add_test(
    NAME osSystemTests
    COMMAND osSystemTests
)

add_executable(osTimeTests "osTimeTests.cpp")
DEP_ext_gtest(osTimeTests)
target_link_libraries(osTimeTests PRIVATE os util)
add_test(
    NAME osTimeTests
    COMMAND osTimeTests
)
