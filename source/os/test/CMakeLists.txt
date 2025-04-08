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
    target_link_libraries(osAtomicTests os util gtest_main)
    add_test(
        NAME osAtomicTests
        COMMAND osAtomicTests
    )

    add_executable(osDirTests "osDirTests.cpp")
    target_link_libraries(osDirTests os util gtest_main)
    add_test(
        NAME osDirTests
        COMMAND osDirTests
    )

    add_executable(osFileTests "osFileTests.cpp")
    target_link_libraries(osFileTests os util gtest_main)
    add_test(
        NAME osFileTests
        COMMAND osFileTests
    )

    add_executable(osEnvTests "osEnvTests.cpp")
    target_link_libraries(osEnvTests os util gtest_main)
    add_test(
        NAME osEnvTests
        COMMAND osEnvTests
    )

    add_executable(osTests "osTests.cpp")
    target_link_libraries(osTests os util gtest_main)
    add_test(
        NAME osTests
        COMMAND osTests
    )

    add_executable(osThreadTests "osThreadTests.cpp")
    target_link_libraries(osThreadTests os util gtest_main)
    add_test(
        NAME osThreadTests
        COMMAND osThreadTests
    )
endif()

add_executable(osMathTests "osMathTests.cpp")
target_link_libraries(osMathTests os util gtest_main)
add_test(
    NAME osMathTests
    COMMAND osMathTests
)

add_executable(osSemaphoreTests "osSemaphoreTests.cpp")
target_link_libraries(osSemaphoreTests os util gtest_main)
add_test(
    NAME osSemaphoreTests
    COMMAND osSemaphoreTests
)

add_executable(osSleepTests "osSleepTests.cpp")
target_link_libraries(osSleepTests os util gtest_main)
add_test(
    NAME osSleepTests
    COMMAND osSleepTests
)

if(NOT TD_WINDOWS)
add_executable(osStringTests "osStringTests.cpp")
target_link_libraries(osStringTests os util gtest_main)
add_test(
    NAME osStringTests
    COMMAND osStringTests
)
endif()

add_executable(osSystemTests "osSystemTests.cpp")
target_link_libraries(osSystemTests os util gtest_main)
add_test(
    NAME osSystemTests
    COMMAND osSystemTests
)

add_executable(osTimeTests "osTimeTests.cpp")
target_link_libraries(osTimeTests os util gtest_main)
add_test(
    NAME osTimeTests
    COMMAND osTimeTests
)
