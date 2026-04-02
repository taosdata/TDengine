# Create library target
add_library(plugin_tdengine STATIC
  src/TDengineRegistrar.cpp
  src/SqlInsertDataFormatter.cpp
  src/StmtInsertDataFormatter.cpp
  src/TDengineSinkPlugin.cpp
)

# Set include directories
target_include_directories(plugin_tdengine
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(plugin_tdengine
  PUBLIC
    parameter
    core_insert
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()