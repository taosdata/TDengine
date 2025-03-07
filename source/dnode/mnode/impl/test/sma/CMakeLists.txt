aux_source_directory(. MNODE_SMA_TEST_SRC)
add_executable(test_sma ${MNODE_SMA_TEST_SRC})
target_link_libraries(
  test_sma
  PUBLIC sut
)

if(NOT ${TD_WINDOWS})
  add_test(
    NAME test_sma
    COMMAND test_sma
  )
endif(NOT ${TD_WINDOWS})
