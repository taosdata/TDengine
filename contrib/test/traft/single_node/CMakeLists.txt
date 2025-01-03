add_executable(singleNode "")
add_dependencies(singleNode ext_libuv)
target_include_directories(singleNode PRIVATE "${TD_CONTRIB_EXT_LIBUV_INC_DIR}")
target_link_directories(singleNode PRIVATE "${TD_CONTRIB_EXT_LIBUV_LIB_DIR}")
target_sources(singleNode
  PRIVATE
  "singleNode.c" 
)
target_link_libraries(singleNode PUBLIC traft lz4 uv.a)
