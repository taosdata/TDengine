
aux_source_directory(src TOPICTOOL_SRC)

add_executable(topic-test-tool ${TOPICTOOL_SRC})

DEP_ext_cjson(topic-test-tool)

target_link_libraries(
    topic-test-tool
    PRIVATE pthread util common ${TAOS_NATIVE_LIB_STATIC}
)

target_include_directories(
    topic-test-tool
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(topic-test-tool PROPERTIES OUTPUT_NAME topic-producer)
