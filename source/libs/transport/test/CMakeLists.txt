add_executable(transportTest "")
add_executable(transUT "")
add_executable(transUT2 "")
add_executable(svrBench "")
add_executable(cliBench "")
add_executable(httpBench "")
add_executable(transImpl2 "")
add_executable(cliBench2 "")
add_executable(svrBench2 "")

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

target_sources(svrBench2
  PRIVATE
  "svrBench2.c"
)
target_sources(cliBench
  PRIVATE
  "cliBench.c"
)
target_sources(cliBench2
  PRIVATE
  "cliBench2.c"
)
target_sources(httpBench
  PRIVATE
  "http_test.c"
)

target_sources(transImpl2
  PRIVATE
  "transImpl2_test.cpp"
)



target_include_directories(transportTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(transportTest
  os
  util
  common
  gtest_main
  transport
  function
)

target_link_libraries(transUT
  os
  util
  common
  gtest_main
  transport
)

target_include_directories(transUT
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(transUT2
  os
  util
  common
  gtest_main
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

target_link_libraries(svrBench
  os
  util
  common
  gtest_main
  transport
)
target_include_directories(svrBench2
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(svrBench2
  os
  util
  common
  gtest_main
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

target_include_directories(cliBench2
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_include_directories(httpBench
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(transImpl2
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(transImpl2
  os
  util
  common
  gtest_main
  transport
  function
)

target_link_libraries(cliBench
  os
  util
  common
  gtest_main
  transport
)

target_link_libraries(svrBench
  os
  util
  common
  gtest_main
  transport
)

target_link_libraries(cliBench2
  os
  util
  common
  gtest_main
  transport
)
target_link_libraries(svrBench2
  os
  util
  common
  gtest_main
  transport
)

target_link_libraries(httpBench
  os
  util
  common
  gtest_main
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
