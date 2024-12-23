MESSAGE(STATUS "build decimal unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(decimalTest ${SOURCE_LIST})

TARGET_INCLUDE_DIRECTORIES(
        decimalTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/decimal/"
)

TARGET_LINK_LIBRARIES(
        decimalTest
        PUBLIC os util common gtest qcom
)
