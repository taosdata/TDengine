
MESSAGE(STATUS "build parser unit test")

# IF(NOT TD_DARWIN)
# # GoogleTest requires at least C++11
# SET(CMAKE_CXX_STANDARD 11)
# AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
#
# ADD_EXECUTABLE(test_executor ${SOURCE_LIST})
# TARGET_LINK_LIBRARIES(
# test_executor
# PRIVATE os util common transport gtest ${TAOS_LIB_STATIC} qcom executor function planner scalar nodes vnode
# )
#
# TARGET_INCLUDE_DIRECTORIES(
# test_executor
# PUBLIC "${TD_SOURCE_DIR}/include/libs/executor/"
# PRIVATE "${TD_SOURCE_DIR}/source/libs/executor/inc"
# )
# ENDIF ()
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(test_join joinTests.cpp)
TARGET_LINK_LIBRARIES(
  test_join
  PRIVATE sut
)

TARGET_INCLUDE_DIRECTORIES(
  test_join
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

ADD_EXECUTABLE(test_queryplan queryPlanTests.cpp)
TARGET_LINK_LIBRARIES(
  test_queryplan
  PRIVATE sut
)

TARGET_INCLUDE_DIRECTORIES(
  test_queryplan
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

ADD_EXECUTABLE(test_executil execUtilTests.cpp)
TARGET_LINK_LIBRARIES(
  test_executil
  PRIVATE sut
)

TARGET_INCLUDE_DIRECTORIES(
  test_executil
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
