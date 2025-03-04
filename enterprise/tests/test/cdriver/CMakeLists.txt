PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)

IF(TD_LINUX)
  add_executable(synctest ./synctest.c)
  target_link_libraries(synctest taos_static pthread)

  # add_executable(benchmark_by_table benchmark_by_table.c)
  # target_link_libraries(benchmark_by_table taos_static  pthread)
ENDIF()
