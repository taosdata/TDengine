if(TD_LINUX)
  aux_source_directory(. TCS_TEST_SRC)

  add_executable(test_tcs ${TCS_TEST_SRC})
  target_include_directories(test_tcs
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/tcs"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_link_libraries(test_tcs
    tcs
    gtest_main
  )
  enable_testing()
  add_test(
    NAME test_tcs
    COMMAND test_tcs
  )
endif()
