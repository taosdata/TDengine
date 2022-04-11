
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(clientTest clientTests.cpp)
TARGET_LINK_LIBRARIES(
        clientTest
        PUBLIC os util common transport parser catalog scheduler function gtest taos qcom
)

ADD_EXECUTABLE(tmqTest tmqTest.cpp)
TARGET_LINK_LIBRARIES(
        tmqTest
        PUBLIC os util common transport parser catalog scheduler function gtest taos qcom
)

TARGET_INCLUDE_DIRECTORIES(
        clientTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        tmqTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/client/inc"
)
