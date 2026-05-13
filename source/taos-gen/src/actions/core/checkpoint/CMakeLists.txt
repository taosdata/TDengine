# Create library target
add_library(core_checkpoint STATIC
  src/CheckpointAction.cpp
)

# Set include directories
target_include_directories(core_checkpoint
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../base
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/connector/inc
)

# Link dependencies
target_link_libraries(core_checkpoint
  PUBLIC
    actions_config
    components_connector
    utils
    nlohmann_json::nlohmann_json # Add dependency for JSON parsing
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()