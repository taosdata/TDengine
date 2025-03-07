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
  add_executable(test_osatomic "osAtomicTests.cpp")
  target_link_libraries(test_osatomic sut)
  add_test(
    NAME test_osatomic
    COMMAND test_osatomic
  )

  add_executable(test_osdir "osDirTests.cpp")
  target_link_libraries(test_osdir sut)
  add_test(
    NAME test_osdir
    COMMAND test_osdir
  )

  add_executable(test_osfile "osFileTests.cpp")
  target_link_libraries(test_osfile sut)
  add_test(
    NAME test_osfile
    COMMAND test_osfile
  )

  add_executable(test_osenv "osEnvTests.cpp")
  target_link_libraries(test_osenv sut)
  add_test(
    NAME test_osenv
    COMMAND test_osenv
  )

  add_executable(test_ostest "osTests.cpp")
  target_link_libraries(test_ostest sut)
  add_test(
    NAME test_ostest
    COMMAND test_ostest
  )

  add_executable(test_osthread "osThreadTests.cpp")
  target_link_libraries(test_osthread sut)
  add_test(
    NAME test_osthread
    COMMAND test_osthread
  )
endif()

add_executable(test_osmath "osMathTests.cpp")
target_link_libraries(test_osmath sut)
add_test(
  NAME test_osmath
  COMMAND test_osmath
)

add_executable(test_ossemaphore "osSemaphoreTests.cpp")
target_link_libraries(test_ossemaphore sut)
add_test(
  NAME test_ossemaphore
  COMMAND test_ossemaphore
)

add_executable(test_ossleep "osSleepTests.cpp")
target_link_libraries(test_ossleep sut)
add_test(
  NAME test_ossleep
  COMMAND test_ossleep
)

add_executable(test_osstring "osStringTests.cpp")
target_link_libraries(test_osstring sut)
add_test(
  NAME test_osstring
  COMMAND test_osstring
)

add_executable(test_ossystem "osSystemTests.cpp")
target_link_libraries(test_ossystem sut)
add_test(
  NAME test_ossystem
  COMMAND test_ossystem
)

add_executable(test_ostime "osTimeTests.cpp")
target_link_libraries(test_ostime sut)
add_test(
  NAME test_ostime
  COMMAND test_ostime
)
