# Add submodules
add_subdirectory(src/generator)
add_subdirectory(src/pipeline)
add_subdirectory(src/writer)

# Create library target
add_library(core_insert STATIC
  src/InsertDataAction.cpp
  $<TARGET_OBJECTS:insert_generator>
  $<TARGET_OBJECTS:insert_pipeline>
  $<TARGET_OBJECTS:insert_writer>
)

# Set include directories
target_include_directories(core_insert
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../base
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/metrics/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/garbage_collector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/formatter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/reader/csv/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pipeline/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/writer/inc
)

# Link dependent libraries
target_link_libraries(core_insert
  PUBLIC
    insert_generator
    insert_pipeline
    insert_writer
    actions_config
    components_metrics
    components_memory_pool
    components_gc
    components_connector
    components_formatter
    components_generator
    components_reader
    utils
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()