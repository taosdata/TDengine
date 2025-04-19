aux_source_directory(src DND_SUT_SRC)
add_library(sut STATIC STATIC ${DND_SUT_SRC})
DEP_ext_gtest(sut)
target_include_directories(
    sut
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    sut PUBLIC dnode
)
