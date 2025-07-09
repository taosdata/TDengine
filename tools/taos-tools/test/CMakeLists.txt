
MESSAGE(STATUS "build taos-tools unit test")

IF(TD_LINUX)

    # GoogleTest requires at least C++11
    # SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    # benchmark
    SET(BENCHMARK_SRC 
        "../src/benchUtil.c"
        "../src/benchLog.c"
        "../src/toolstime.c"
        "../../src/pub.c"
    )

    ADD_EXECUTABLE(benchmarkTest benchmarkTest.cpp ${BENCHMARK_SRC})
    DEP_ext_gtest(benchmarkTest)

    TARGET_LINK_LIBRARIES(
            benchmarkTest
            PRIVATE taos toolscJson
    )

    target_include_directories(
        benchmarkTest PRIVATE 
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
        "${CMAKE_CURRENT_SOURCE_DIR}/../deps/toolscJson/inc/"
    )
    
    add_test(
        NAME benchmarkTest
        COMMAND benchmarkTest
    )

    # taosdump
    ADD_EXECUTABLE(taosdumpTest taosdumpTest.cpp ../src/toolsSys.c)
    DEP_ext_gtest(taosdumpTest)
    target_link_libraries(
        taosdumpTest
        PRIVATE os
    )

    target_include_directories(
        taosdumpTest
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
    
    add_test(
        NAME taosdumpTest
        COMMAND taosdumpTest
    )

ENDIF()
