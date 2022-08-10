MESSAGE(STATUS "build command unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(commandTest ${SOURCE_LIST})

TARGET_INCLUDE_DIRECTORIES(
        commandTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/command/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/command/inc"
)

TARGET_LINK_LIBRARIES(
        commandTest
        PUBLIC os util common nodes parser catalog transport gtest function qcom
)