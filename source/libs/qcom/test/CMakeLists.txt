
MESSAGE(STATUS "build qcom unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(queryUtilTest ${SOURCE_LIST})

TARGET_INCLUDE_DIRECTORIES(
        queryUtilTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/qcom/inc"
)

TARGET_LINK_LIBRARIES(
        queryUtilTest
        PUBLIC os util gtest qcom common transport
)
