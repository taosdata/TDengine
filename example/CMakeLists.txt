aux_source_directory(src TMQ_DEMO_SRC)

add_executable(tmq ${TMQ_DEMO_SRC})
target_link_libraries(
    tmq taos
)
target_include_directories(
    tmq
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(tmq PROPERTIES OUTPUT_NAME tmq)
