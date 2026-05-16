# Test DataPipeline
add_executable(TestDataPipeline
  TestDataPipeline.cpp
)
target_link_libraries(TestDataPipeline
  PRIVATE
    insert_pipeline
)
add_test(NAME TestDataPipeline COMMAND TestDataPipeline)