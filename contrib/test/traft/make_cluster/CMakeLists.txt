add_executable(makeCluster "")
target_sources(makeCluster 
  PRIVATE
  "raftMain.c" 
  "raftServer.c"
  "config.c"
  "console.c"
  "simpleHash.c"
  "util.c"
)
target_link_libraries(makeCluster PUBLIC traft lz4 uv_a)
