CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)

SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O3")

IF (TD_LINUX)
  add_executable(taoswechai ./taoswechai.c )
  target_link_libraries(taoswechai taos pthread)
ENDIF()
