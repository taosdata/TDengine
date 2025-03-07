if(TD_LINUX)
  aux_source_directory(. AZ_TEST_SRC)

  add_executable(test_az ${AZ_TEST_SRC})
  target_include_directories(test_az
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/azure"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_link_libraries(test_az
    az
    gtest_main
  )
  enable_testing()
  add_test(
    NAME az_test
    COMMAND test_az
  )
endif(TD_LINUX)
