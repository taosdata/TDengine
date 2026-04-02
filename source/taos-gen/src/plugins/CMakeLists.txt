# Find dependent packages

# Add interface library
add_library(plugin_base INTERFACE)
target_include_directories(plugin_base
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/tdengine/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/mqtt/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/kafka/inc
)

# Add subdirectories for each plugin
add_subdirectory(src/tdengine)
add_subdirectory(src/mqtt)
add_subdirectory(src/kafka)

# Create library target
add_library(plugins STATIC
  src/PluginRegistrar.cpp
  $<TARGET_OBJECTS:plugin_tdengine>
  $<TARGET_OBJECTS:plugin_mqtt>
  $<TARGET_OBJECTS:plugin_kafka>
)

# Set include directories
target_include_directories(plugins
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/../workflow/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/../actions/core/checkpoint/inc
    # ${CMAKE_CURRENT_SOURCE_DIR}/../actions/base
    # ${PROJECT_SOURCE_DIR}/src
)

# Link dependent libraries
target_link_libraries(plugins
  PUBLIC
    plugin_base
    plugin_tdengine
    plugin_mqtt
    plugin_kafka
    # actions_config
    # core_checkpoint
    # yaml-cpp::yaml-cpp
)

# Enable tests
# if(TSGEN_ENABLE_TEST)
#   add_subdirectory(test)
# endif()