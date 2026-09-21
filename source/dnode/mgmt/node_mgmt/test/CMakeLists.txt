add_executable(streamHeartbeatTest streamHeartbeatTest.cpp)
DEP_ext_gtest(streamHeartbeatTest)
target_include_directories(streamHeartbeatTest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc")
target_link_libraries(streamHeartbeatTest PRIVATE dnode)
target_link_options(streamHeartbeatTest PRIVATE
  -Wl,--wrap=rpcOpen
  -Wl,--wrap=rpcFreeCont
  -Wl,--wrap=dmMarkWrapper
  -Wl,--wrap=dmReleaseWrapper
  -Wl,--wrap=streamHbProcessRspMsg
  -Wl,--wrap=streamHbHandleRspErr
)
add_test(NAME streamHeartbeatTest COMMAND streamHeartbeatTest)
