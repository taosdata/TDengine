# Test ColumnConverter
add_executable(TestColumnConverter
  TestColumnConverter.cpp
)
target_link_libraries(TestColumnConverter
  PRIVATE 
    components_memory_pool
)
add_test(NAME TestColumnConverter COMMAND TestColumnConverter)

# Test MemoryPool
add_executable(TestMemoryPool
  TestMemoryPool.cpp
)
target_link_libraries(TestMemoryPool
  PRIVATE 
    components_memory_pool
)
add_test(NAME TestMemoryPool COMMAND TestMemoryPool)
