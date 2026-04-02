# Test TimeIntervalStrategy
add_executable(TestTimeIntervalStrategy
  TestTimeIntervalStrategy.cpp
)
target_link_libraries(TestTimeIntervalStrategy
  PRIVATE
    insert_writer
)
add_test(NAME TestTimeIntervalStrategy COMMAND TestTimeIntervalStrategy)

# Test SinkPluginFactory
add_executable(TestSinkPluginFactory
  TestSinkPluginFactory.cpp
)
target_link_libraries(TestSinkPluginFactory
  PRIVATE
    insert_writer
)
add_test(NAME TestSinkPluginFactory COMMAND TestSinkPluginFactory)