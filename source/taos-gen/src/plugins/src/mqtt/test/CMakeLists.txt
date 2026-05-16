# Test MqttConfigParser
add_executable(TestMqttConfigParser
  TestMqttConfigParser.cpp
)
target_link_libraries(TestMqttConfigParser
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttConfigParser COMMAND TestMqttConfigParser)

# Test MqttParameterContext
add_executable(TestMqttParameterContext
  TestMqttParameterContext.cpp
)
target_link_libraries(TestMqttParameterContext
  PRIVATE
    plugins
)
add_test(NAME TestMqttParameterContext COMMAND TestMqttParameterContext)

# Test MqttInsertData
add_executable(TestMqttInsertData
  TestMqttInsertData.cpp
)
target_link_libraries(TestMqttInsertData
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttInsertData COMMAND TestMqttInsertData)

# Test MqttInsertDataFormatter
add_executable(TestMqttInsertDataFormatter
  TestMqttInsertDataFormatter.cpp
)
target_link_libraries(TestMqttInsertDataFormatter
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttInsertDataFormatter COMMAND TestMqttInsertDataFormatter)

# Test MqttClient
add_executable(TestMqttClient
  TestMqttClient.cpp
)
target_link_libraries(TestMqttClient
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttClient COMMAND TestMqttClient)

# Test MqttSinkPlugin
add_executable(TestMqttSinkPlugin
  TestMqttSinkPlugin.cpp
)
target_link_libraries(TestMqttSinkPlugin
  PRIVATE
    plugin_mqtt
)
add_test(NAME TestMqttSinkPlugin COMMAND TestMqttSinkPlugin)
