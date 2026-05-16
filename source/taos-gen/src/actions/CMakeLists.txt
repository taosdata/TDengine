# Add interface library
add_library(actions_base INTERFACE)
target_include_directories(actions_base
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/base
)

# Add config module
add_subdirectory(config)

# Add components directory
add_subdirectory(components)

# Add core module
add_subdirectory(core)

# Create unified actions library
add_library(actions STATIC
  $<TARGET_OBJECTS:actions_config>
  $<TARGET_OBJECTS:actions_components>
  $<TARGET_OBJECTS:actions_core>
)

# Set include directories for actions library
target_include_directories(actions
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/base
    ${CMAKE_CURRENT_SOURCE_DIR}/config/inc
)

# Link submodules
target_link_libraries(actions
  PUBLIC
    actions_base
    actions_config
    actions_components
    actions_core
)