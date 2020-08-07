CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)
  
IF (TD_LINUX_64)
  add_executable(sync_insert ./sync_insert.c )
  target_link_libraries(sync_insert taos_static pthread)
ENDIF()
