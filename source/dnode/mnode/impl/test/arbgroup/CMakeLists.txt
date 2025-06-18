SET(CMAKE_CXX_STANDARD 11)

aux_source_directory(. MNODE_ARBGROUP_TEST_SRC)
add_executable(arbgroupTest ${MNODE_ARBGROUP_TEST_SRC})
DEP_ext_gtest(arbgroupTest)
target_link_libraries(
        arbgroupTest
        PRIVATE dnode nodes planner qcom tss
)

add_test(
        NAME arbgroupTest
        COMMAND arbgroupTest
)
