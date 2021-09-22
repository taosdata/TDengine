aux_source_directory(src ASVC_SRC)
add_library(asvc ${ASVC_SRC})
target_include_directories(
    asvc
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/asvc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)