aux_source_directory(src SUT_SRC)
add_library(sut STATIC STATIC ${SUT_SRC})
target_link_libraries(
    sut
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(
    sut
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
