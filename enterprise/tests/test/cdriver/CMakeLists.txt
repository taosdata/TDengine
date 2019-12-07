CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)
INCLUDE_DIRECTORIES(${TD_OS_DIR}/inc)

IF ((TD_LINUX_64) OR (TD_LINUX_32 AND TD_ARM))
  add_executable(synctest ./synctest.c )
  target_link_libraries(synctest taos_static  pthread)

  add_executable(benchmark_by_table benchmark_by_table.c)
  target_link_libraries(benchmark_by_table taos_static  pthread)
ENDIF()
