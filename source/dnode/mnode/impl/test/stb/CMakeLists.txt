aux_source_directory(. MNODE_STB_TEST_SRC)
add_executable(test_stb ${MNODE_STB_TEST_SRC})
target_link_libraries(
  test_stb
  PUBLIC sut
)

if(NOT ${TD_WINDOWS})
  add_test(
    NAME test_stb
    COMMAND test_stb
  )
endif(NOT ${TD_WINDOWS})