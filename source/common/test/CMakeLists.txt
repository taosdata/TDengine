
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)

if(TD_LINUX)
    add_executable(commonTest "commonTests.cpp")
    target_link_libraries(commonTest os util common gtest_main)
    target_include_directories(
        commonTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/common/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/common/inc"
    )
    add_test(
        NAME commonTest
        COMMAND commonTest
    )
endif()

# dataformatTest.cpp
add_executable(dataformatTest "dataformatTest.cpp")
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

# cosCpTest.cpp
add_executable(cosCpTest "cosCpTest.cpp")
target_link_libraries(cosCpTest gtest gtest_main util common)
target_include_directories(
    cosCpTest
    PUBLIC "${TD_SOURCE_DIR}/include/common"
    PUBLIC "${TD_SOURCE_DIR}/include/util"
)
add_test(
    NAME cosCpTest
    COMMAND cosCpTest
)

if(TD_LINUX)
    # cosTest.cpp
    add_executable(cosTest "cosTest.cpp")
    target_link_libraries(cosTest gtest gtest_main util common)
    target_include_directories(
        cosTest
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PUBLIC "${TD_SOURCE_DIR}/include/util"
    )
    add_test(
        NAME cosTest
        COMMAND cosTest
    )
endif()

if(${TD_LINUX})
    # tmsg test
    add_executable(tmsgTest "")
    target_sources(tmsgTest
        PRIVATE
        "tmsgTest.cpp"
        "../src/msg/tmsg.c"
    )
    target_include_directories(tmsgTest PUBLIC "${TD_SOURCE_DIR}/include/common/")
    target_link_libraries(tmsgTest PUBLIC os util gtest gtest_main)
    add_test(
        NAME tmsgTest
        COMMAND tmsgTest
    )

    # config file for msg type table
    SET(MSG_TBL_FILE ${CMAKE_CURRENT_SOURCE_DIR}/msgTypeTable.ini)
    add_custom_command(TARGET tmsgTest POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${MSG_TBL_FILE} $<TARGET_FILE_DIR:tmsgTest>
    )
endif()
