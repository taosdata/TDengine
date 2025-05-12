aux_source_directory(src NODES_SRC)
add_library(nodes STATIC ${NODES_SRC})
target_include_directories(
    nodes
    PUBLIC "${TD_SOURCE_DIR}/include/libs/nodes"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    nodes
    PRIVATE os util common qcom function
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
