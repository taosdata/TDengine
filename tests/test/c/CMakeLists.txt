CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/util/inc)
INCLUDE_DIRECTORIES(${TD_OS_DIR}/inc)

IF ((TD_LINUX_64) OR (TD_LINUX_32 AND TD_ARM))
  add_executable(benchmarkPerTable benchmarkPerTable.c)
  target_link_libraries(benchmarkPerTable taos_static pthread)
ENDIF()
