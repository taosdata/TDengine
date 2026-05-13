# Create library target
add_library(actions_config STATIC
  src/TDengineConfig.cpp
  src/ColumnType.cpp
  src/ColumnConfig.cpp
  src/TypeConverter.cpp
  src/PluginConfigRegistry.cpp
)

# Set include directories
target_include_directories(actions_config
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../utils/inc
)

# Link dependencies
target_link_libraries(actions_config
  PUBLIC
    utils
    yaml-cpp::yaml-cpp
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()