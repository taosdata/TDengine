# Test SubscribeDataAction
add_executable(TestSubscribeDataAction
  TestSubscribeDataAction.cpp
)
target_link_libraries(TestSubscribeDataAction
  PRIVATE
    core_subscribe
    plugin_tdengine
    ${TAOS_LIBRARIES}
)
add_test(NAME TestSubscribeDataAction COMMAND TestSubscribeDataAction)