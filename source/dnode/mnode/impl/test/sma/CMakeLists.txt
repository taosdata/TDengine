aux_source_directory(. SMA_SRC)
add_executable(mnode_test_sma ${SMA_SRC})
target_link_libraries(
    mnode_test_sma
    PUBLIC sut
)

add_test(
    NAME mnode_test_sma
    COMMAND mnode_test_sma
)
