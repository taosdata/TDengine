aux_source_directory(src MGMT_SNODE)
add_library(mgmt_snode STATIC ${MGMT_SNODE})

if(${TD_DARWIN})
    target_compile_options(mgmt_snode PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    mgmt_snode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mgmt_snode node_util
)