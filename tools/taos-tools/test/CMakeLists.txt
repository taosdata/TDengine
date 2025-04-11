
MESSAGE(STATUS "build taos-tools unit test")

IF(TD_LINUX)

    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
    ADD_SUBDIRECTORY("../src/")

    # benchmark
    SET(BENCHMARK_SRC "benchUtil.c")
    ADD_EXECUTABLE(benchmarkTest benchmarkTest.cpp ${BENCHMARK_SRC})
    TARGET_LINK_LIBRARIES(
            benchmarkTest
            PRIVATE gtest 
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
    TARGET_LINK_LIBRARIES(
        taosdumpTest
            PRIVATE gtest 
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
