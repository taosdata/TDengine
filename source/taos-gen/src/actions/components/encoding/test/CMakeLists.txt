# Test EncodingConverter
add_executable(TestEncodingConverter
  TestEncodingConverter.cpp
)
target_link_libraries(TestEncodingConverter
  PRIVATE
    components_encoding
)
add_test(NAME TestEncodingConverter COMMAND TestEncodingConverter)