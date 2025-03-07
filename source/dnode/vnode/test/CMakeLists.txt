MESSAGE(STATUS "tq unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)

IF(NOT TD_WINDOWS)
  add_executable(test_tq tqTest.cpp)
  target_include_directories(test_tq
    PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  TARGET_LINK_LIBRARIES(
    test_tq
    PUBLIC sut
  )

  enable_testing()

  add_test(
    NAME test_tq
    COMMAND test_tq
  )
ENDIF()

# ADD_EXECUTABLE(test_tsdbsma tsdbSmaTest.cpp)
# TARGET_LINK_LIBRARIES(
# test_tsdbsma
# PUBLIC sut
# )

# TARGET_INCLUDE_DIRECTORIES(
# test_tsdbsma
# PUBLIC "${TD_SOURCE_DIR}/include/common"
# PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
# PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )