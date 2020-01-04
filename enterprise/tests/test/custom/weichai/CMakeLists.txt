CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)
INCLUDE_DIRECTORIES(${TD_OS_DIR}/inc)

SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O3")

IF ((TD_LINUX_64) OR (TD_LINUX_32 AND TD_ARM))
  add_executable(taoswechai ./taoswechai.c )
  target_link_libraries(taoswechai taos pthread)
ENDIF()
