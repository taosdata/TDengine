
MESSAGE(STATUS "build taos-tools unit test")

IF (DEFINED TD_VER_NUMBER)
    ADD_DEFINITIONS(-DTD_VER_NUMBER="${TD_VER_NUMBER}")
    ADD_DEFINITIONS(-DTAOSBENCHMARK_COMMIT_ID="${TAOSBENCHMARK_COMMIT_ID}")
    ADD_DEFINITIONS(-DTAOSDUMP_COMMIT_ID="${TAOSDUMP_COMMIT_ID}")
    ADD_DEFINITIONS(-DBUILD_INFO="${BUILD_INFO}")  
    MESSAGE(STATUS "version:${TD_VER_NUMBER}")
ELSE ()    
    # abort build
    MESSAGE(FATAL_ERROR "build taos-tools not found TD_VER_NUMBER define.")
ENDIF ()

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
        "../src/benchData.c"
        "../src/benchInsert.c"
        "../src/benchInsertMix.c"
        "../src/benchUtilDs.c"
        "../src/wrapDb.c"
        "../src/benchDataMix.c"
        "../src/benchJsonOpt.c"
        "../src/benchSys.c"
        "../src/toolsString.c"
        "../src/toolsSys.c"
        "../src/benchCommandOpt.c"
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

    SET(TAOSDUMP_SRC 
        "../src/taosdump.c"
        "../src/toolsSys.c"
        "../../src/pub.c"
        "../src/toolsString.c"
        "../src/wrapDb.c"
        "../src/dumpUtil.c"
        "../src/toolstime.c"
        "../src/toolsDir.c"
        "../src/benchUtil.c"
        "../src/benchLog.c"
        "../src/toolstime.c"
        "../src/benchData.c"
        "../src/benchInsert.c"
        "../src/benchInsertMix.c"
        "../src/benchUtilDs.c"
        "../src/benchDataMix.c"
        "../src/benchCommandOpt.c"
    )
    
    # taosdump
    ADD_EXECUTABLE(taosdumpTest taosdumpTest.cpp ${TAOSDUMP_SRC})
    DEP_ext_gtest(taosdumpTest)
    DEP_ext_avro(taosdumpTest)
    DEP_ext_zlib(taosdumpTest)
    DEP_ext_jansson(taosdumpTest)
    DEP_ext_snappy(taosdumpTest)
    DEP_ext_xz(taosdumpTest)
    target_compile_definitions(taosdumpTest PRIVATE UNIT_TEST)
    target_link_libraries(taosdumpTest PRIVATE os)
    TARGET_LINK_LIBRARIES(taosdumpTest PRIVATE taos)
    target_link_libraries(taosdumpTest PRIVATE toolscJson)
    target_link_libraries(taosdumpTest PRIVATE stdc++)

    target_include_directories(
        taosdumpTest
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../deps/toolscJson/inc/"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../deps/avro/lang/c/src"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../../include/libs/decimal"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../../include/common"
    )

    add_test(
        NAME taosdumpTest
        COMMAND taosdumpTest
    )

ENDIF()
