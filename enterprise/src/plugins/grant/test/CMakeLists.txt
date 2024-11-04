MESSAGE(STATUS "build grant unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)

# grantTest.cpp 
ADD_EXECUTABLE(grantTest "")
TARGET_SOURCES(
    grantTest
    PRIVATE
    "grantTest.cpp"
)
TARGET_LINK_LIBRARIES(
        grantTest
        PUBLIC os util common gtest gtest_main
)

TARGET_INCLUDE_DIRECTORIES(
        grantTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/common/"
        PUBLIC "${TD_SOURCE_DIR}/include/util"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/common/inc"
)

if (${TD_LINUX})
    add_test(
        NAME grantTest
        COMMAND grantTest
    )
endif ()