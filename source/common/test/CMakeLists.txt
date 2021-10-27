
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(commonTest ${SOURCE_LIST})
TARGET_LINK_LIBRARIES(
        commonTest
        PUBLIC os util common gtest
)

TARGET_INCLUDE_DIRECTORIES(
        commonTest
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/common/"
        PRIVATE "${CMAKE_SOURCE_DIR}/source/libs/common/inc"
)
