CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/os/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/util/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/common/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/query/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/mnode/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/tsdb/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/plugins/http/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/inc)
INCLUDE_DIRECTORIES(${TD_ENTERPRISE_DIR}/src/inc)

IF (TD_LINUX)
  #add_executable(insertPerTable insertPerTable.c)
  #target_link_libraries(insertPerTable taos_static pthread)

  #add_executable(insertPerRow insertPerRow.c)
  #target_link_libraries(insertPerRow taos_static pthread)

  #add_executable(importOneRow importOneRow.c)
  #target_link_libraries(importOneRow taos_static pthread)

  #add_executable(importPerTable importPerTable.c)
  #target_link_libraries(importPerTable taos_static pthread)

  #add_executable(hashPerformance hashPerformance.c)
  #target_link_libraries(hashPerformance taos_static tutil common pthread)

  #add_executable(createTablePerformance createTablePerformance.c)
  #target_link_libraries(createTablePerformance taos_static tutil common pthread)

  #add_executable(createNormalTable createNormalTable.c)
  #target_link_libraries(createNormalTable taos_static tutil common pthread)

  #add_executable(queryPerformance queryPerformance.c)
  #target_link_libraries(queryPerformance taos_static tutil common pthread)
  
  #add_executable(httpTest httpTest.c)
  #target_link_libraries(httpTest taos_static tutil common pthread mnode monitor http tsdb twal vnode cJson lz4)

  #add_executable(cacheTest cacheTest.c)
  #target_link_libraries(cacheTest taos_static tutil common pthread mnode monitor http tsdb twal vnode cJson lz4)

  #add_executable(invalidTableId invalidTableId.c)
  #target_link_libraries(invalidTableId taos_static tutil common pthread)

  #add_executable(hashIterator hashIterator.c)
  #target_link_libraries(hashIterator taos_static tutil common pthread)
ENDIF()

