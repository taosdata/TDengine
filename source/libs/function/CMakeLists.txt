aux_source_directory(src FUNCTION_SRC)
add_library(function ${FUNCTION_SRC})
target_include_directories(
    function
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/function"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    function
    PRIVATE os util common
)