# Test benchmark cli
add_executable(TestBenchmarkCLI
  TestBenchmarkCLI.cpp
)
target_link_libraries(TestBenchmarkCLI
  PRIVATE
)
add_test(NAME TestBenchmarkCLI COMMAND TestBenchmarkCLI)