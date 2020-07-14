CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/util/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/common/inc)
INCLUDE_DIRECTORIES(${TD_OS_DIR}/inc)

IF ((TD_LINUX_64) OR (TD_LINUX_32 AND TD_ARM))
  add_executable(tdengineTest tdengineTest.c)
  target_link_libraries(tdengineTest taos_static tutil common pthread)
ENDIF()
