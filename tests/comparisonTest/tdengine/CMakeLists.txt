CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

IF (TD_LINUX_64)
  add_executable(tdengineTest tdengineTest.c)
  target_link_libraries(tdengineTest taos_static tutil common pthread)
ENDIF()
