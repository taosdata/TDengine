
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(parserTest ${SOURCE_LIST})
TARGET_LINK_LIBRARIES(
        parserTest
        PUBLIC os util common parser catalog transport gtest function
)

TARGET_INCLUDE_DIRECTORIES(
        parserTest
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/parser/"
        PRIVATE "${CMAKE_SOURCE_DIR}/source/libs/parser/inc"
)
