
MESSAGE(STATUS "build parser unit test")

# IF(NOT TD_DARWIN)
#         # GoogleTest requires at least C++11
#         SET(CMAKE_CXX_STANDARD 11)
#         AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
# 
#         ADD_EXECUTABLE(executorTest ${SOURCE_LIST})
#         TARGET_LINK_LIBRARIES(
#                 executorTest
#                 PRIVATE os util common transport gtest taos_static qcom executor function planner scalar nodes vnode
#         )
# 
#         TARGET_INCLUDE_DIRECTORIES(
#                 executorTest
#                 PUBLIC "${TD_SOURCE_DIR}/include/libs/executor/"
#                 PRIVATE "${TD_SOURCE_DIR}/source/libs/executor/inc"
#         )
# ENDIF ()

SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(joinTests joinTests.cpp)
TARGET_LINK_LIBRARIES(
        joinTests
        PRIVATE os util common executor gtest_main qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        joinTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

ADD_EXECUTABLE(queryPlanTests queryPlanTests.cpp)
TARGET_LINK_LIBRARIES(
        queryPlanTests
        PRIVATE os util common executor gtest_main qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        queryPlanTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
