MESSAGE(STATUS "vnode unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

# add_executable(tqTest "")
# target_sources(tqTest
#     PRIVATE
#     "tqMetaTest.cpp"
# )
# target_include_directories(tqTest
#     PUBLIC
#     "${TD_SOURCE_DIR}/include/server/vnode/tq"
#     "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )

# target_link_libraries(tqTest
#     tq
#     gtest_main
# )
# enable_testing()
# add_test(
#     NAME tq_test
#     COMMAND tqTest
# )

# ADD_EXECUTABLE(tsdbSmaTest tsdbSmaTest.cpp)
# TARGET_LINK_LIBRARIES(
#         tsdbSmaTest
#         PUBLIC os util common vnode gtest_main
# )

# TARGET_INCLUDE_DIRECTORIES(
#         tsdbSmaTest
#         PUBLIC "${TD_SOURCE_DIR}/include/common"
#         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
#         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )

ADD_EXECUTABLE(headdump headdump.c)

aux_source_directory("../src/tsdb/" DUMP_TSDB_SOURCE_FILES)

target_sources(
    headdump
    PRIVATE
    ${DUMP_TSDB_SOURCE_FILES}
)

target_include_directories(
    headdump
    PUBLIC "${TD_SOURCE_DIR}/source/dnode/vnode/src/tsdb"
)

TARGET_LINK_LIBRARIES(
        headdump
        PUBLIC os util common vnode
)
