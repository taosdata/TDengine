# Find dependent packages
find_package(RdKafka REQUIRED)

# Create library target
add_library(plugin_kafka STATIC
  src/KafkaRegistrar.cpp
  src/KafkaInsertDataFormatter.cpp
  src/KafkaClient.cpp
  src/KafkaSinkPlugin.cpp
)

# Set include directories
target_include_directories(plugin_kafka
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(plugin_kafka
  PUBLIC
    parameter
    core_insert
    RdKafka::rdkafka++
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()