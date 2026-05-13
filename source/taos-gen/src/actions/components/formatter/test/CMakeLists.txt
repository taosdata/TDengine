# test SqlDatabaseFormatter
add_executable(TestSqlDatabaseFormatter
  TestSqlDatabaseFormatter.cpp
)
target_link_libraries(TestSqlDatabaseFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlDatabaseFormatter COMMAND TestSqlDatabaseFormatter)

# test SqlSuperTableFormatter
add_executable(TestSqlSuperTableFormatter
  TestSqlSuperTableFormatter.cpp
)
target_link_libraries(TestSqlSuperTableFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlSuperTableFormatter COMMAND TestSqlSuperTableFormatter)

# test SqlChildTableFormatter
add_executable(TestSqlChildTableFormatter
  TestSqlChildTableFormatter.cpp
)
target_link_libraries(TestSqlChildTableFormatter
  PRIVATE
    components_formatter
)
add_test(NAME TestSqlChildTableFormatter COMMAND TestSqlChildTableFormatter)

# test FormatResult
add_executable(TestFormatResult
  TestFormatResult.cpp
)
target_link_libraries(TestFormatResult
  PRIVATE
    components_formatter
)
add_test(NAME TestFormatResult COMMAND TestFormatResult)

# Test PatternGenerator
add_executable(TestPatternGenerator
  TestPatternGenerator.cpp
)
target_link_libraries(TestPatternGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestPatternGenerator COMMAND TestPatternGenerator)

# Test TopicGenerator
add_executable(TestTopicGenerator
  TestTopicGenerator.cpp
)
target_link_libraries(TestTopicGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestTopicGenerator COMMAND TestTopicGenerator)

# Test KeyGenerator
add_executable(TestKeyGenerator
  TestKeyGenerator.cpp
)
target_link_libraries(TestKeyGenerator
  PRIVATE
    insert_writer
)
add_test(NAME TestKeyGenerator COMMAND TestKeyGenerator)

# Test RowSerializer
add_executable(TestRowSerializer
  TestRowSerializer.cpp
)
target_link_libraries(TestRowSerializer
  PRIVATE
    insert_writer
)
add_test(NAME TestRowSerializer COMMAND TestRowSerializer)
