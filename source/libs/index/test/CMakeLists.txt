IF(NOT TD_DARWIN)
  add_executable(test_idx "")
  add_executable(test_idxfst1 "")
  add_executable(test_idxfst2 "")
  add_executable(test_idxutil "")
  add_executable(test_idxjson "")
  add_executable(test_idxfstutil "")

  target_sources(test_idx
    PRIVATE
    "indexTests.cc"
  )
  target_sources(test_idxfst1
    PRIVATE
    "fstTest.cc"
  )

  target_sources(test_idxfst2
    PRIVATE
    "fstUT.cc"
  )
  target_sources(test_idxutil
    PRIVATE
    "utilUT.cc"
  )

  target_sources(test_idxjson
    PRIVATE
    "jsonUT.cc"
  )
  target_sources(test_idxfstutil
    PRIVATE
    "fstUtilUT.cc"
  )

  target_include_directories(test_idx
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_include_directories(test_idxfst1
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_sources(test_idxjson
    PRIVATE
    "jsonUT.cc"
  )
  target_include_directories(test_idx
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_include_directories(test_idxfst1
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_include_directories(test_idxfst2
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_include_directories(test_idxutil
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_include_directories(test_idxjson
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_include_directories(test_idxfstutil
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_include_directories(test_idxjson
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_link_libraries(test_idx
    os
    util
    common
    gtest_main
    index
  )
  target_link_libraries(test_idxfst1
    os
    util
    common
    gtest_main
    index
  )
  target_link_libraries(test_idxfst2
    os
    util
    common
    gtest_main
    index
  )

  target_link_libraries(test_idx
    os
    util
    common
    gtest_main
    index
  )
  target_link_libraries(test_idxfst1
    os
    util
    common
    gtest_main
    index
  )
  target_link_libraries(test_idxfst2
    os
    util
    common
    gtest_main
    index
  )

  target_link_libraries(test_idxutil
    os
    util
    common
    gtest_main
    index
  )

  target_link_libraries(test_idxjson
    os
    util
    common
    gtest_main
    index
  )
  target_link_libraries(test_idxfstutil
    os
    util
    common
    gtest_main
    index
  )

  add_test(
    NAME test_idxjson
    COMMAND test_idxjson
  )
  add_test(
    NAME test_idxfstutil
    COMMAND test_idxfstutil
  )

  add_test(
    NAME test_idx
    COMMAND test_idx
  )
  add_test(
    NAME test_idxutil
    COMMAND test_idxutil
  )
  add_test(
    NAME test_idxfst2
    COMMAND test_idxfst2
  )
  add_test(
    NAME test_idxfst1
    COMMAND test_idxfst1
  )
ENDIF()
