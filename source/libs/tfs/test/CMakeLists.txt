enable_testing()

aux_source_directory(. TFS_TEST_SRC)
add_executable(tfs_test ${TFS_TEST_SRC})
if(${FALLBACK})                # {
  target_link_libraries(
    tfs_test
    PUBLIC tfs
    PUBLIC gtest_main
  )
else()                         # }{
  target_link_libraries(
    tfs_test
    PUBLIC tfs
  )
  DEP_ext_gtest(tfs_test)
  DEP_ext_cjson_LIB(tfs_test)
endif()                        # }
target_include_directories(
  tfs_test
  PUBLIC "${TD_SOURCE_DIR}/include/libs/tfs"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

add_test(
    NAME tfs_test
    COMMAND tfs_test
)
