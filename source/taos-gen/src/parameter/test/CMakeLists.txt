# test ParseYAML
add_executable(TestParseYAML
  TestParseYAML.cpp
)
target_link_libraries(TestParseYAML
  PRIVATE
    yaml-cpp::yaml-cpp
)
target_compile_definitions(TestParseYAML PRIVATE
    CONFIG_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../conf/config.yaml"
)
add_test(NAME TestParseYAML COMMAND TestParseYAML)

# test ConfigParser
add_executable(TestConfigParser
  TestConfigParser.cpp
)
target_link_libraries(TestConfigParser
  PRIVATE
    parameter
    plugins
)
add_test(NAME TestConfigParser COMMAND TestConfigParser)

# test ParameterContext
add_executable(TestParameterContext
  TestParameterContext.cpp
)
target_link_libraries(TestParameterContext
  PRIVATE
    parameter
    plugins
)
add_test(NAME TestParameterContext COMMAND TestParameterContext)