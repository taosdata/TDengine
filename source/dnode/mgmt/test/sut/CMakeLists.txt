aux_source_directory(src DND_SUT_SRC)
add_library(sut STATIC STATIC ${DND_SUT_SRC})
target_include_directories(
    sut
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(${FALLBACK})        # {
    target_link_libraries(
        sut dnode gtest_main
    )
else()                 # }{
    target_link_libraries(
        sut PUBLIC dnode
    )
    DEP_ext_gtest(sut)
endif()                # }