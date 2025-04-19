# CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
# PROJECT(TDengine)


IF(TD_LINUX)
    ADD_EXECUTABLE(simTests "simTests.cpp")
    DEP_ext_gtest(simTests)
    TARGET_INCLUDE_DIRECTORIES(simTests PRIVATE ${TD_SOURCE_DIR}/src/util/inc)
    TARGET_LINK_LIBRARIES(simTests PRIVATE os util tsim_static)

    ADD_TEST(
        NAME simTests
        COMMAND simTests
    )
ENDIF()
