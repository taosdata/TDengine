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
add_test(
    NAME heapTest
    COMMAND heapTest
)

# arrayTest
add_executable(arrayTest "arrayTest.cpp")
DEP_ext_gtest(arrayTest)
target_link_libraries(arrayTest PRIVATE os util)
add_test(
    NAME arrayTest
    COMMAND arrayTest
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


if(${TD_LINUX})
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
endif()
