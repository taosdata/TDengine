add_executable(transportTest "")
add_executable(transUT "")
add_executable(transUT2 "")
add_executable(svrBench "")
add_executable(cliBench "")
add_executable(httpBench "")

target_sources(transUT
  PRIVATE
  "transUT.cpp"
)

target_sources(transUT2
  PRIVATE
  "transUT2.cpp"
)
target_sources(transportTest
  PRIVATE
  "transportTests.cpp"
)

target_sources(svrBench
  PRIVATE
  "svrBench.c"
)
target_sources(cliBench
  PRIVATE
  "cliBench.c"
)
target_sources(httpBench
  PRIVATE
  "http_test.c"
)

target_include_directories(transportTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

DEP_ext_gtest(transportTest)
target_link_libraries(transportTest PRIVATE
  os
  util
  common
  transport
  function
)

DEP_ext_gtest(transUT)
target_link_libraries(transUT PRIVATE
  os
  util
  common
  transport
)

target_include_directories(transUT
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

DEP_ext_gtest(transUT2)
target_link_libraries(transUT2 PRIVATE
  os
  util
  common
  transport
)

target_include_directories(transUT2
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_include_directories(svrBench
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

DEP_ext_gtest(svrBench)
target_link_libraries(svrBench PRIVATE
  os
  util
  common
  transport
)

target_include_directories(cliBench
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_include_directories(httpBench
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

DEP_ext_gtest(cliBench)
target_link_libraries(cliBench PRIVATE
  os
  util
  common
  transport
)

DEP_ext_gtest(httpBench)
target_link_libraries(httpBench PRIVATE
  os
  util
  common
  transport
)

add_test(
  NAME transUT
  COMMAND transUT
)
add_test(
  NAME transUtilUt
  COMMAND transportTest
)
