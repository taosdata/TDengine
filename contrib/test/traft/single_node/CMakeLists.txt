add_executable(singleNode "")
target_sources(singleNode
  PRIVATE
  "singleNode.c" 
)
target_link_libraries(singleNode traft lz4 uv_a)
