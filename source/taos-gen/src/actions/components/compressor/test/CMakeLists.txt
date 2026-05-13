# Test Compressor
add_executable(TestCompressor
  TestCompressor.cpp
)
target_link_libraries(TestCompressor
  PRIVATE
    components_compressor
)
add_test(NAME TestCompressor COMMAND TestCompressor)