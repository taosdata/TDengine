# Test KafkaConfigParser
add_executable(TestKafkaConfigParser
  TestKafkaConfigParser.cpp
)
target_link_libraries(TestKafkaConfigParser
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaConfigParser COMMAND TestKafkaConfigParser)

# Test KafkaParameterContext
add_executable(TestKafkaParameterContext
  TestKafkaParameterContext.cpp
)
target_link_libraries(TestKafkaParameterContext
  PRIVATE
    plugins
)
add_test(NAME TestKafkaParameterContext COMMAND TestKafkaParameterContext)

# Test KafkaInsertData
add_executable(TestKafkaInsertData
  TestKafkaInsertData.cpp
)
target_link_libraries(TestKafkaInsertData
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaInsertData COMMAND TestKafkaInsertData)

# Test KafkaInsertDataFormatter
add_executable(TestKafkaInsertDataFormatter
  TestKafkaInsertDataFormatter.cpp
)
target_link_libraries(TestKafkaInsertDataFormatter
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaInsertDataFormatter COMMAND TestKafkaInsertDataFormatter)

# Test KafkaClient
add_executable(TestKafkaClient
  TestKafkaClient.cpp
)
target_link_libraries(TestKafkaClient
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaClient COMMAND TestKafkaClient)

# Test KafkaSinkPlugin
add_executable(TestKafkaSinkPlugin
  TestKafkaSinkPlugin.cpp
)
target_link_libraries(TestKafkaSinkPlugin
  PRIVATE
    plugin_kafka
)
add_test(NAME TestKafkaSinkPlugin COMMAND TestKafkaSinkPlugin)
