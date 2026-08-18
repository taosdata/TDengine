SET(CMAKE_CXX_STANDARD 11)

add_executable(streamTest "stream.cpp")
DEP_ext_gtest(streamTest)
DEP_ext_cppstub(streamTest)
target_link_libraries(
        streamTest
        PRIVATE mnode
)
target_include_directories(
        streamTest
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)

if(NOT TD_WINDOWS)
        add_test(
                NAME streamTest
                COMMAND streamTest
        )
endif(NOT TD_WINDOWS)

# aux_source_directory(. MNODE_STREAM_TEST_SRC)
# add_executable(streamTest ${MNODE_STREAM_TEST_SRC})
# DEP_ext_gtest(streamTest)
# target_link_libraries(
#         streamTest
#         PRIVATE dnode nodes planner qcom
# )
# 
# add_test(
#         NAME streamTest
#         COMMAND streamTest
# )
