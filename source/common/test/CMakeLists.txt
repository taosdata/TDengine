
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(commonTest "")
TARGET_SOURCES(
    commonTest
    PRIVATE
    "commonTests.cpp"
)
TARGET_LINK_LIBRARIES(
        commonTest
        PUBLIC os util common gtest
)

TARGET_INCLUDE_DIRECTORIES(
        commonTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/common/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/common/inc"
)

# dataformatTest.cpp 
add_executable(dataformatTest "")
target_sources(
    dataformatTest
    PRIVATE
    "dataformatTest.cpp"
)
target_link_libraries(dataformatTest gtest gtest_main util common)
target_include_directories(
        dataformatTest
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PUBLIC "${TD_SOURCE_DIR}/include/util"
)
add_test(
    NAME dataformatTest
    COMMAND dataformatTest
)

# tmsg test
# add_executable(tmsgTest "")
# target_sources(tmsgTest 
#         PRIVATE
#         "tmsgTest.cpp"
#         "../src/tmsg.c"
# )
# target_include_directories(tmsgTest PUBLIC "${TD_SOURCE_DIR}/include/common/")
# target_link_libraries(tmsgTest PUBLIC os util gtest gtest_main)