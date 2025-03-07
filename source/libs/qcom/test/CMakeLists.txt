MESSAGE(STATUS "build qcom unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(test_queryutil ${SOURCE_LIST})

TARGET_INCLUDE_DIRECTORIES(
  test_queryutil
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom/"
  PRIVATE "${TD_SOURCE_DIR}/source/libs/qcom/inc"
)

TARGET_LINK_LIBRARIES(
  test_queryutil
  PUBLIC os util gtest qcom common transport
)
