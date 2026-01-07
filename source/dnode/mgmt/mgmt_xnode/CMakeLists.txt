aux_source_directory(src MGMT_XNODE)
add_library(mgmt_xnode STATIC ${MGMT_XNODE})

if(${TD_DARWIN})
    target_compile_options(mgmt_xnode PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
  mgmt_xnode
  PUBLIC "${TD_SOURCE_DIR}/include/dnode/xnode"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    mgmt_xnode xnode txnode node_util
)
