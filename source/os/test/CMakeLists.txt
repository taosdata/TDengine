CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

if(${FALLBACK})          # {
  FIND_PATH(HEADER_GTEST_INCLUDE_DIR gtest.h /usr/include/gtest /usr/local/include/gtest)
  FIND_LIBRARY(LIB_GTEST_STATIC_DIR libgtest.a /usr/lib/ /usr/local/lib /usr/lib64)
  FIND_LIBRARY(LIB_GTEST_SHARED_DIR libgtest.so /usr/lib/ /usr/local/lib /usr/lib64)

  IF(HEADER_GTEST_INCLUDE_DIR AND(LIB_GTEST_STATIC_DIR OR LIB_GTEST_SHARED_DIR))
      MESSAGE(STATUS "gTest library found, build os test")

      INCLUDE_DIRECTORIES(${HEADER_GTEST_INCLUDE_DIR})
      AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
  ENDIF()
endif()                  # }

if(NOT ${FALLBACK})      # {
    SET(CMAKE_CXX_STANDARD 14)
endif()                  # }

INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/src/util/inc)

if(TD_LINUX)
add_executable(osAtomicTests "osAtomicTests.cpp")
if(${FALLBACK})           # {
  target_link_libraries(osAtomicTests os util gtest_main)
else()                    # }{
  target_link_libraries(osAtomicTests PUBLIC os util)
  DEP_ext_gtest(osAtomicTests)
endif()                   # }
add_test(
    NAME osAtomicTests
    COMMAND osAtomicTests
)
endif()

if(TD_LINUX)
add_executable(osDirTests "osDirTests.cpp")
if(${FALLBACK})           # {
  target_link_libraries(osDirTests os util gtest_main)
else()                    # }{
  target_link_libraries(osDirTests PUBLIC os util)
  DEP_ext_gtest(osDirTests)
endif()                   # }
add_test(
    NAME osDirTests
    COMMAND osDirTests
)
endif()

if(TD_LINUX)
add_executable(osEnvTests "osEnvTests.cpp")
if(${FALLBACK})           # {
  target_link_libraries(osEnvTests os util gtest_main)
else()                    # }{
  target_link_libraries(osEnvTests PUBLIC os util)
  DEP_ext_gtest(osEnvTests)
endif()                   # }
add_test(
    NAME osEnvTests
    COMMAND osEnvTests
)
endif()

add_executable(osMathTests "osMathTests.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(osMathTests os util gtest_main)
else()                          # }{
    target_link_libraries(osMathTests PUBLIC os util)
    DEP_ext_gtest(osMathTests)
endif()                         # }
add_test(
    NAME osMathTests
    COMMAND osMathTests
)

add_executable(osSemaphoreTests "osSemaphoreTests.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(osSemaphoreTests os util gtest_main)
else()                          # }{
    target_link_libraries(osSemaphoreTests PUBLIC os util)
    DEP_ext_gtest(osSemaphoreTests)
endif()                         # }
add_test(
    NAME osSemaphoreTests
    COMMAND osSemaphoreTests
)

add_executable(osSignalTests "osSignalTests.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(osSignalTests os util gtest_main)
else()                          # }{
    target_link_libraries(osSignalTests PUBLIC os util)
    DEP_ext_gtest(osSignalTests)
endif()                         # }
add_test(
    NAME osSignalTests
    COMMAND osSignalTests
)

add_executable(osSleepTests "osSleepTests.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(osSleepTests os util gtest_main)
else()                          # }{
    target_link_libraries(osSleepTests PUBLIC os util)
    DEP_ext_gtest(osSleepTests)
endif()                         # }
add_test(
    NAME osSleepTests
    COMMAND osSleepTests
)

add_executable(osStringTests "osStringTests.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(osStringTests os util gtest_main)
else()                          # }{
    target_link_libraries(osStringTests PUBLIC os util)
    DEP_ext_gtest(osStringTests)
endif()                         # }
add_test(
    NAME osStringTests
    COMMAND osStringTests
)

add_executable(osTests "osTests.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(osTests os util gtest_main)
else()                          # }{
    target_link_libraries(osTests PUBLIC os util)
    DEP_ext_gtest(osTests)
endif()                         # }
add_test(
    NAME osTests
    COMMAND osTests
)

add_executable(osSystemTests "osSystemTests.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(osSystemTests os util gtest_main)
else()                          # }{
    target_link_libraries(osSystemTests PUBLIC os util)
    DEP_ext_gtest(osSystemTests)
endif()                         # }
add_test(
    NAME osSystemTests
    COMMAND osSystemTests
)

if(TD_LINUX)
add_executable(osThreadTests "osThreadTests.cpp")
if(${FALLBACK})           # {
  target_link_libraries(osThreadTests os util gtest_main)
else()                    # }{
  target_link_libraries(osThreadTests PUBLIC os util)
  DEP_ext_gtest(osThreadTests)
endif()                   # }
add_test(
    NAME osThreadTests
    COMMAND osThreadTests
)
endif()

add_executable(osTimeTests "osTimeTests.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(osTimeTests os util gtest_main)
else()                          # }{
    target_link_libraries(osTimeTests PUBLIC os util)
    DEP_ext_gtest(osTimeTests)
endif()                         # }
add_test(
    NAME osTimeTests
    COMMAND osTimeTests
)

