add_executable(test_wal "walMetaTest.cpp")
target_include_directories(test_wal
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/wal"
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(test_wal
  wal
  sut
)

add_test(
  NAME test_wal
  COMMAND test_wal
)
