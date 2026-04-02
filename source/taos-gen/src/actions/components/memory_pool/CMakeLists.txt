# Create library target
add_library(components_memory_pool STATIC
  src/ColumnConverter.cpp
  src/MemoryPool.cpp
)

# Set include directories
target_include_directories(components_memory_pool
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
)

# Link dependent libraries
target_link_libraries(components_memory_pool
  PUBLIC
    actions_config
    utils
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()