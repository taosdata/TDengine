# Test QueryDataAction
add_executable(TestQueryDataAction
  TestQueryDataAction.cpp
)
target_link_libraries(TestQueryDataAction
  PRIVATE
    core_query
    plugin_tdengine
    ${TAOS_LIBRARIES}
)
add_test(NAME TestQueryDataAction COMMAND TestQueryDataAction)