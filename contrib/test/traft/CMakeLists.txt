add_executable(raftMain "")
target_sources(raftMain 
  PRIVATE
  "raftMain.c" 
  "raftServer.c"
)
target_link_libraries(raftMain PUBLIC traft lz4 uv_a)
