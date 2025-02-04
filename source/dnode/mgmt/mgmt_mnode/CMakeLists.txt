aux_source_directory(src MGMT_MNODE)
add_library(mgmt_mnode STATIC ${MGMT_MNODE})
target_include_directories(
    mgmt_mnode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
if(${FALLBACK})               # {
  target_link_libraries(
      mgmt_mnode node_util
  )
else()                        # }{
  target_link_libraries(
      mgmt_mnode PUBLIC node_util
  )
  DEP_ext_cjson_LIB(mgmt_mnode)
endif()                       # }

