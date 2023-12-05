aux_source_directory(. MNODE_STREAM_PLAN_TEST_SRC)
add_executable(streamPlanTest ${MNODE_STREAM_PLAN_TEST_SRC})
target_link_libraries(
    streamPlanTest
    PUBLIC nodes planner gtest qcom
)

add_test(
    NAME streamPlanTest
    COMMAND streamPlanTest

)
