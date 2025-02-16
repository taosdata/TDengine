if(${FALLBACK})          # {
    SET(CMAKE_CXX_STANDARD 11)
else()                   # }{
    SET(CMAKE_CXX_STANDARD 14)
endif()                  # }

aux_source_directory(. MNODE_STREAM_TEST_SRC)
add_executable(streamTest ${MNODE_STREAM_TEST_SRC})
if(${FALLBACK})                # {
        target_link_libraries(
                streamTest
                PRIVATE dnode nodes planner gtest qcom
        )
else()                         # }{
        target_link_libraries(
                streamTest
                PRIVATE dnode nodes planner qcom
        )
        DEP_ext_gtest(streamTest)
endif()                        # }

add_test(
        NAME streamTest
        COMMAND streamTest
)
