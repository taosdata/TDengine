# Test TimestampGenerator
add_executable(TestTimestampGenerator
  TestTimestampGenerator.cpp
)
target_link_libraries(TestTimestampGenerator
  PRIVATE
    components_generator
)
add_test(NAME TestTimestampGenerator COMMAND TestTimestampGenerator)

# Test TableNameGenerator
add_executable(TestTableNameGenerator
  TestTableNameGenerator.cpp
)
target_link_libraries(TestTableNameGenerator
  PRIVATE
    components_generator
)
add_test(NAME TestTableNameGenerator COMMAND TestTableNameGenerator)

# Test OrderColumnGenerator
add_executable(TestOrderColumnGenerator
  TestOrderColumnGenerator.cpp
)
target_link_libraries(TestOrderColumnGenerator
  PRIVATE
    components_generator
)
add_test(NAME TestOrderColumnGenerator COMMAND TestOrderColumnGenerator)

# Test RandomColumnGenerator
add_executable(TestRandomColumnGenerator
  TestRandomColumnGenerator.cpp
)
target_link_libraries(TestRandomColumnGenerator
  PRIVATE
    components_generator
)
add_test(NAME TestRandomColumnGenerator COMMAND TestRandomColumnGenerator)

# Test ExprColumnGenerator
add_executable(TestExprColumnGenerator
  TestExprColumnGenerator.cpp
)
target_link_libraries(TestExprColumnGenerator
  PRIVATE
    components_generator
)
add_test(NAME TestExprColumnGenerator COMMAND TestExprColumnGenerator)

# Test ColumnGeneratorFactory
add_executable(TestColumnGeneratorFactory
  TestColumnGeneratorFactory.cpp
)
target_link_libraries(TestColumnGeneratorFactory
  PRIVATE
    components_generator
)
add_test(NAME TestColumnGeneratorFactory COMMAND TestColumnGeneratorFactory)

# Test RowGenerator
add_executable(TestRowGenerator
  TestRowGenerator.cpp
)
target_link_libraries(TestRowGenerator
  PRIVATE
    components_generator
)
add_test(NAME TestRowGenerator COMMAND TestRowGenerator)