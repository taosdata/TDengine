MESSAGE(STATUS "build external connector boundary unit test")

SET(CMAKE_CXX_STANDARD 11)

ADD_EXECUTABLE(extConnectorBoundaryTest extConnectorBoundaryTest.cpp)
DEP_ext_gtest(extConnectorBoundaryTest)

TARGET_INCLUDE_DIRECTORIES(
  extConnectorBoundaryTest
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

TARGET_LINK_LIBRARIES(
  extConnectorBoundaryTest
  PUBLIC extconnector os util common qcom nodes crypt
)

add_test(
  NAME extConnectorBoundaryTest
  COMMAND extConnectorBoundaryTest
)
