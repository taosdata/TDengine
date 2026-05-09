# Test InfluxDBInsertDataFormatter
add_executable(TestInfluxDBInsertDataFormatter
  TestInfluxDBInsertDataFormatter.cpp
)
target_link_libraries(TestInfluxDBInsertDataFormatter
  PRIVATE
    plugin_influxdb
)
add_test(NAME TestInfluxDBInsertDataFormatter COMMAND TestInfluxDBInsertDataFormatter)

# Test InfluxDBClient
add_executable(TestInfluxDBClient
  TestInfluxDBClient.cpp
)
target_link_libraries(TestInfluxDBClient
  PRIVATE
    plugin_influxdb
)
add_test(NAME TestInfluxDBClient COMMAND TestInfluxDBClient)

# Test InfluxDBConfigParser
add_executable(TestInfluxDBConfigParser
  TestInfluxDBConfigParser.cpp
)
target_link_libraries(TestInfluxDBConfigParser
  PRIVATE
    plugin_influxdb
)
add_test(NAME TestInfluxDBConfigParser COMMAND TestInfluxDBConfigParser)

# Test InfluxDBSinkPlugin
add_executable(TestInfluxDBSinkPlugin
  TestInfluxDBSinkPlugin.cpp
)
target_link_libraries(TestInfluxDBSinkPlugin
  PRIVATE
    plugin_influxdb
)
add_test(NAME TestInfluxDBSinkPlugin COMMAND TestInfluxDBSinkPlugin)
