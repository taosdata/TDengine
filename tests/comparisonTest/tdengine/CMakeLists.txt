CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
PROJECT(TDengine)

IF (TD_LINUX)
  add_executable(tdengineTest tdengineTest.c)
  target_link_libraries(tdengineTest taos_static tutil common pthread)
ENDIF()

IF (TD_DARWIN)
  add_executable(tdengineTest tdengineTest.c)
  target_link_libraries(tdengineTest taos_static tutil common pthread)
ENDIF()
