# Add subcomponent directories
add_subdirectory(create)
add_subdirectory(insert)
add_subdirectory(checkpoint)
add_subdirectory(query)
add_subdirectory(subscribe)

# Create unified component library
add_library(actions_core STATIC
  $<TARGET_OBJECTS:core_create>
  $<TARGET_OBJECTS:core_insert>
  $<TARGET_OBJECTS:core_checkpoint>
  $<TARGET_OBJECTS:core_query>
  $<TARGET_OBJECTS:core_subscribe>
)

# Set include directories
target_include_directories(actions_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/create/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/insert/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/query/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/subscribe/inc
)

# Link submodules
target_link_libraries(actions_core
  PUBLIC
    core_create
    core_insert
    core_checkpoint
    core_query
    core_subscribe
)