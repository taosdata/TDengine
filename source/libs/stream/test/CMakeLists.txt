
MESSAGE(STATUS "build stream unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

# bloomFilterTest
ADD_EXECUTABLE(streamUpdateTest "tstreamUpdateTest.cpp")

TARGET_LINK_LIBRARIES(streamUpdateTest
        PUBLIC os util common gtest gtest_main stream executor index
        )

TARGET_INCLUDE_DIRECTORIES(
  streamUpdateTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
  PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
)

add_test(
  NAME streamUpdateTest
  COMMAND streamUpdateTest
)