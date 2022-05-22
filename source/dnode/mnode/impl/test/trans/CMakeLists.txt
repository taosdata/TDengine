# add_executable(transTest1 "")
# target_sources(transTest1
#     PRIVATE
#     "${CMAKE_CURRENT_SOURCE_DIR}/trans1.cpp"
# )
# target_link_libraries(
#     transTest1
#     PUBLIC sut
# )
# target_include_directories(
#     transTest1
#     PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
#     PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
# )
# add_test(
#     NAME transTest1
#     COMMAND transTest1
# )

add_executable(transTest2 "")
target_sources(transTest2
    PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/trans2.cpp"
)
target_link_libraries(
    transTest2
    PUBLIC dnode mnode gtest_main
)
target_include_directories(
    transTest2
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)
#add_test(
#    NAME transTest2
#    COMMAND transTest2
#)
