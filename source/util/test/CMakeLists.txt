# CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
# PROJECT(TDengine)

# IF (TD_LINUX)
# ADD_EXECUTABLE(trefTest ./trefTest.c)
# TARGET_LINK_LIBRARIES(trefTest util common)
# ENDIF ()
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/source/util/inc)
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/common)

add_executable(heapTest "heapTest.cpp")
DEP_ext_gtest(heapTest)
target_link_libraries(heapTest PRIVATE os util)
if(NOT TD_WINDOWS)
    add_test(
        NAME heapTest
        COMMAND heapTest
    )
endif()

# arrayTest
add_executable(arrayTest "arrayTest.cpp")
DEP_ext_gtest(arrayTest)
target_link_libraries(arrayTest PRIVATE os util)
add_test(
    NAME arrayTest
    COMMAND arrayTest
)

# tconvTest
add_executable(tconvTest "tconvTest.cpp")
DEP_ext_gtest(tconvTest)
target_link_libraries(tconvTest PRIVATE os util)
add_test(
    NAME tconvTest
    COMMAND tconvTest
)

# # freelistTest
# add_executable(freelistTest "")
# target_sources(freelistTest
# PRIVATE
# "freelistTest.cpp"
# )
# target_link_libraries(freelistTest os util gtest gtest_main)

# # encodeTest
# add_executable(encodeTest "encodeTest.cpp")
# target_link_libraries(encodeTest os util gtest gtest_main)

# cfgTest
add_executable(cfgTest "cfgTest.cpp")
DEP_ext_gtest(cfgTest)
target_link_libraries(cfgTest PRIVATE os util)
add_test(
    NAME cfgTest
    COMMAND cfgTest
)

# bloomFilterTest
add_executable(bloomFilterTest "bloomFilterTest.cpp")
DEP_ext_gtest(bloomFilterTest)
target_link_libraries(bloomFilterTest PRIVATE os util)
add_test(
    NAME bloomFilterTest
    COMMAND bloomFilterTest
)

# taosbsearchTest
add_executable(taosbsearchTest "taosbsearchTest.cpp")
DEP_ext_gtest(taosbsearchTest)
target_link_libraries(taosbsearchTest PRIVATE os util)
add_test(
    NAME taosbsearchTest
    COMMAND taosbsearchTest
)

# trbtreeTest
add_executable(rbtreeTest "trbtreeTest.cpp")
DEP_ext_gtest(rbtreeTest)
target_link_libraries(rbtreeTest PRIVATE os util)
add_test(
    NAME rbtreeTest
    COMMAND rbtreeTest
)

# pageBufferTest
add_executable(pageBufferTest "pageBufferTest.cpp")
DEP_ext_gtest(pageBufferTest)
target_link_libraries(pageBufferTest PRIVATE os util)
add_test(
    NAME pageBufferTest
    COMMAND pageBufferTest
)

# talgoTest
add_executable(talgoTest "talgoTest.cpp")
DEP_ext_gtest(talgoTest)
target_link_libraries(talgoTest PRIVATE os util)
add_test(
    NAME talgoTest
    COMMAND talgoTest
)

# tbaseCodecTest
add_executable(tbaseCodecTest "tbaseCodecTest.cpp")
DEP_ext_gtest(tbaseCodecTest)
target_link_libraries(tbaseCodecTest PRIVATE os util common)
add_test(
    NAME tbaseCodecTest
    COMMAND tbaseCodecTest
)

# bufferTest
add_executable(bufferTest "bufferTest.cpp")
DEP_ext_gtest(bufferTest)
target_link_libraries(bufferTest PRIVATE os util)
add_test(
    NAME bufferTest
    COMMAND bufferTest
)

add_executable(regexTest "regexTest.cpp")
DEP_ext_gtest(regexTest)
target_link_libraries(regexTest PRIVATE os util)
add_test(
    NAME regexTest
    COMMAND regexTest
)

add_executable(lrucacheTest "lrucacheTest.cpp")
DEP_ext_gtest(lrucacheTest)
target_link_libraries(lrucacheTest PRIVATE os util)
add_test(
    NAME lrucacheTest
    COMMAND lrucacheTest
)

add_executable(logTest "log.cpp")
DEP_ext_gtest(logTest)
target_link_libraries(logTest PRIVATE os util common)
add_test(
    NAME logTest
    COMMAND logTest
)
target_include_directories(
  logTest
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

add_executable(decompressTest "decompressTest.cpp")
DEP_ext_gtest(decompressTest)
target_link_libraries(decompressTest PRIVATE os util common)
add_test(
    NAME decompressTest
    COMMAND decompressTest
)

add_executable(objpoolTest "objpoolTest.cpp")
DEP_ext_gtest(objpoolTest)
target_link_libraries(objpoolTest PRIVATE os util common)
add_test(
    NAME objpoolTest
    COMMAND objpoolTest
)

# timerTest
add_executable(timerTest "timerTest.cpp")
DEP_ext_gtest(timerTest)
target_link_libraries(timerTest PRIVATE os util)
add_test(
    NAME timerTest
    COMMAND timerTest
)

if(TD_LINUX)
    add_executable(utilTests utilTests.cpp stringTest.cpp)
    DEP_ext_gtest(utilTests)
    target_link_libraries(utilTests PRIVATE os util common)
    add_test(
        NAME utilTests
        COMMAND utilTests
    )

    # terrorTest
    add_executable(terrorTest "terrorTest.cpp")
    DEP_ext_gtest(terrorTest)
    target_link_libraries(terrorTest PRIVATE os util common)
    add_test(
        NAME terrorTest
        COMMAND terrorTest
    )

    # config
    SET(ERR_TBL_FILE ${CMAKE_CURRENT_SOURCE_DIR}/errorCodeTable.ini)
    add_custom_command(TARGET terrorTest POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${ERR_TBL_FILE} $<TARGET_FILE_DIR:terrorTest>
    )

    # memPoolTest
    add_executable(memPoolTest "memPoolTest.cpp")
    DEP_ext_gtest(memPoolTest)
    DEP_ext_cppstub(memPoolTest)
    target_link_libraries(memPoolTest PRIVATE os util common)
    add_test(
        NAME memPoolTest
        COMMAND memPoolTest
    )

    # queryAutoQWorkerTest
    add_executable(queryAutoQWorkerTest "queryAutoQWorkerTest.cpp")
    DEP_ext_gtest(queryAutoQWorkerTest)
    target_link_libraries(queryAutoQWorkerTest PRIVATE os util common)
    add_test(
        NAME queryAutoQWorkerTest
        COMMAND queryAutoQWorkerTest
    )
endif()

# compressBench -- manual micro-benchmark / verifier for the L2 compression
# dispatch table (stock vs. runtime-loaded accel backends). It is a C program
# with its own main(), not a gtest, so it is built without DEP_ext_gtest.
# Linux-only: links libm and calls libc clock_gettime directly.
# Deliberately NOT registered via add_test(): it is a manual tool and must stay
# out of ctest / CI for now. Run the built binary by hand, e.g.
#   ./compressBench --codec all --size 1m --iters 30 --warmup 5 --shape mixed
if(TD_LINUX)
    add_executable(compressBench "compressBench.c")
    target_link_libraries(compressBench PRIVATE util m)
    target_include_directories(
        compressBench
        PRIVATE
            ${TD_SOURCE_DIR}/include/util
            ${TD_SOURCE_DIR}/include/common
            ${TD_SOURCE_DIR}/include/os
    )
endif()

# compressAccelTest -- verifies that the accelerated L2 backends are really
# loaded: points TAOS_COMPRESS_ACCEL_ZLIB at the system libz (resolved at
# runtime via dlopen/dlinfo, so no hard-coded path), then checks that the
# dispatch table was rebound and that compress/decompress round-trips and
# interoperates with the stock implementation. Skips when the system libz is
# absent. Requires the accel code to be compiled in.
if(TD_LINUX AND BUILD_WITH_ACCEL_COMPRESS)
    add_executable(compressAccelTest "compressAccelTest.cpp")
    DEP_ext_gtest(compressAccelTest)
    target_link_libraries(compressAccelTest PRIVATE os util common)
    add_test(
        NAME compressAccelTest
        COMMAND compressAccelTest
    )
endif()
