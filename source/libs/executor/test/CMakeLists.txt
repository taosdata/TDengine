
MESSAGE(STATUS "build parser unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(executorTest ${SOURCE_LIST})
        TARGET_LINK_LIBRARIES(
                executorTest
                PRIVATE os util common transport gtest taos_static qcom executor function planner scalar nodes vnode
        )

        TARGET_INCLUDE_DIRECTORIES(
                executorTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/executor/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/executor/inc"
        )
ENDIF ()

# SET(CMAKE_CXX_STANDARD 11)
# AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

# ADD_EXECUTABLE(tSimpleHashTest tSimpleHashTests.cpp)
# TARGET_LINK_LIBRARIES(
#         tSimpleHashTest
#         PRIVATE os util common executor gtest_main
# )

# TARGET_INCLUDE_DIRECTORIES(
#         tSimpleHashTest
#         PUBLIC "${TD_SOURCE_DIR}/include/common"
#         PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )