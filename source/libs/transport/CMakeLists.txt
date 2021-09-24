aux_source_directory(src TRANSPORT_SRC)
add_library(transport ${TRANSPORT_SRC})
target_include_directories(
    transport
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/transport"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)