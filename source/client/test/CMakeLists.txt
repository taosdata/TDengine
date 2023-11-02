
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(clientTest clientTests.cpp)
TARGET_LINK_LIBRARIES(
        clientTest
        os util common transport parser catalog scheduler gtest taos_static qcom executor function
)

ADD_EXECUTABLE(tmqTest tmqTest.cpp)
TARGET_LINK_LIBRARIES(
        tmqTest
        PUBLIC os util common transport parser catalog scheduler function gtest taos_static qcom
)

ADD_EXECUTABLE(smlTest smlTest.cpp)
TARGET_LINK_LIBRARIES(
        smlTest
        PUBLIC os util common transport parser catalog scheduler function gtest taos_static qcom geometry
)

ADD_EXECUTABLE(clientMonitorTest clientMonitorTests.cpp)
TARGET_LINK_LIBRARIES(
        clientMonitorTest
        PUBLIC os util common transport monitor parser catalog scheduler function gtest taos_static qcom executor
)

TARGET_INCLUDE_DIRECTORIES(
        clientTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        tmqTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        smlTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        clientMonitorTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

add_test(
        NAME smlTest
        COMMAND smlTest
)

# add_test(
#         NAME clientMonitorTest
#         COMMAND clientMonitorTest
# )
