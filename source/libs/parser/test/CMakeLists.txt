
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(astTest ${SOURCE_LIST})
TARGET_LINK_LIBRARIES(
        astTest
        PUBLIC os util common parser catalog transport gtest
)

TARGET_INCLUDE_DIRECTORIES(
        astTest
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/parser/"
        PRIVATE "${CMAKE_SOURCE_DIR}/source/libs/parser/inc"
)
