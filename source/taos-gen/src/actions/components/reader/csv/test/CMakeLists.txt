# Test CSVReader
add_executable(TestCSVReader
  TestCSVReader.cpp
)
target_link_libraries(TestCSVReader
  PRIVATE
    reader_csv
)
add_test(NAME TestCSVReader COMMAND TestCSVReader)

# Test FilePathResolver
add_executable(TestFilePathResolver
  TestFilePathResolver.cpp
)
target_link_libraries(TestFilePathResolver
  PRIVATE
    reader_csv
)
add_test(NAME TestFilePathResolver COMMAND TestFilePathResolver)

# Test TableNameCSVReader
add_executable(TestTableNameCSVReader
  TestTableNameCSVReader.cpp
)
target_link_libraries(TestTableNameCSVReader
  PRIVATE
    reader_csv
)
add_test(NAME TestTableNameCSVReader COMMAND TestTableNameCSVReader)

# Test TagsCSVReader
add_executable(TestTagsCSVReader
  TestTagsCSVReader.cpp
)
target_link_libraries(TestTagsCSVReader
  PRIVATE
    reader_csv
)
add_test(NAME TestTagsCSVReader COMMAND TestTagsCSVReader)

# Test ColumnsCSVReader
add_executable(TestColumnsCSVReader
  TestColumnsCSVReader.cpp
)
target_link_libraries(TestColumnsCSVReader
  PRIVATE
    reader_csv
)
add_test(NAME TestColumnsCSVReader COMMAND TestColumnsCSVReader)

# Test CSVDataManager
add_executable(TestCSVDataManager
  TestCSVDataManager.cpp
)
target_link_libraries(TestCSVDataManager
  PRIVATE
    reader_csv
)
add_test(NAME TestCSVDataManager COMMAND TestCSVDataManager)

# Test StreamingCSVRowSource
add_executable(TestStreamingCSVRowSource
  TestStreamingCSVRowSource.cpp
)
target_link_libraries(TestStreamingCSVRowSource
  PRIVATE
    reader_csv
)
add_test(NAME TestStreamingCSVRowSource COMMAND TestStreamingCSVRowSource)

# Test PreloadCSVRowSource
add_executable(TestPreloadCSVRowSource
  TestPreloadCSVRowSource.cpp
)
target_link_libraries(TestPreloadCSVRowSource
  PRIVATE
    reader_csv
)
add_test(NAME TestPreloadCSVRowSource COMMAND TestPreloadCSVRowSource)