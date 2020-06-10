CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/util/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/common/inc)
INCLUDE_DIRECTORIES(${TD_OS_DIR}/inc)

IF ((TD_LINUX_64) OR (TD_LINUX_32 AND TD_ARM))
  add_executable(insertPerTable insertPerTable.c)
  target_link_libraries(insertPerTable taos_static pthread)

  add_executable(insertPerRow insertPerRow.c)
  target_link_libraries(insertPerRow taos_static pthread)

  add_executable(importOneRow importOneRow.c)
  target_link_libraries(importOneRow taos_static pthread)

  add_executable(importPerTable importPerTable.c)
  target_link_libraries(importPerTable taos_static pthread)
ENDIF()
