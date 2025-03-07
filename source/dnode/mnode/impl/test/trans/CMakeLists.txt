# add_executable(transTest1 "")
# target_sources(transTest1
# PRIVATE
# "${CMAKE_CURRENT_SOURCE_DIR}/trans1.cpp"
# )
# target_link_libraries(
# transTest1
# PUBLIC sut
# )
# target_include_directories(
# transTest1
# PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
# PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
# )
# add_test(
# NAME transTest1
# COMMAND transTest1
# )

add_executable(test_trans2 "")
target_sources(test_trans2
  PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/trans2.cpp"
)
target_link_libraries(
  test_trans2
  PUBLIC sut
)
target_include_directories(
  test_trans2
  PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)

# add_test(
# NAME test_trans2
# COMMAND test_trans2
# )
