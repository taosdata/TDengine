aux_source_directory(source TKV_SRC)
add_library(tkv ${TKV_SRC})
target_include_directories(
    tkv
    PUBLIC "${CMAKE_SOURCE_DIR}/include/tkv"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include"
)