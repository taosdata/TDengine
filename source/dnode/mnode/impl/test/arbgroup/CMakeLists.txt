SET(CMAKE_CXX_STANDARD 11)

aux_source_directory(. MNODE_ARBGROUP_TEST_SRC)
add_executable(arbgroupTest ${MNODE_ARBGROUP_TEST_SRC})
if(${FALLBACK})             # {
        target_link_libraries(
                arbgroupTest
                PRIVATE dnode nodes planner gtest qcom tcs
        )
else()                      # }{
        target_link_libraries(
                arbgroupTest
                PRIVATE dnode nodes planner qcom tcs
        )
        DEP_ext_gtest(arbgroupTest)
        DEP_ext_cjson_LIB(arbgroupTest)
endif()                     # }

add_test(
        NAME arbgroupTest
        COMMAND arbgroupTest
)
