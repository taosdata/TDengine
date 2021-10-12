aux_source_directory(src TKV_SRC)
add_library(tkv ${TKV_SRC})
target_include_directories(
    tkv
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/tkv"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    tkv
    PUBLIC os
)