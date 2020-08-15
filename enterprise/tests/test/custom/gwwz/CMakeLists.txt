CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)
  
IF (TD_LINUX)
  add_executable(taossplit ./taossplit.c )
  target_link_libraries(taossplit taos_static pthread)
ENDIF()
