# Create library target
add_library(components_metrics STATIC
  src/ActionMetrics.cpp
)

# Set include directories
target_include_directories(components_metrics
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependencies
target_link_libraries(components_metrics
  PUBLIC
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()