aux_source_directory(src MGMT_DNODE)
add_library(mgmt_dnode STATIC ${MGMT_DNODE})
if (DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()
target_include_directories(
    mgmt_dnode
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PUBLIC "${GRANT_CFG_INCLUDE_DIR}"
)
target_link_libraries(
    mgmt_dnode node_util
)