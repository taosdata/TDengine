add_executable(rebalanceLeader "")
target_sources(rebalanceLeader 
  PRIVATE
  "raftMain.c" 
  "raftServer.c"
)
target_link_libraries(rebalanceLeader PUBLIC traft lz4 uv_a)
