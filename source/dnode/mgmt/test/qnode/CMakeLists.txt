aux_source_directory(. DQTEST_SRC)
add_executable(dqnodeTest ${DQTEST_SRC})
target_link_libraries(
    dqnodeTest
    PUBLIC sut
)

add_test(
    NAME dqnodeTest
    COMMAND dqnodeTest
)
