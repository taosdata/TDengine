# Find dependent packages
find_package(PahoMqttCpp REQUIRED)

# Create library target
add_library(plugin_mqtt STATIC
  src/MqttRegistrar.cpp
  src/MqttInsertDataFormatter.cpp
  src/MqttClient.cpp
  src/MqttSinkPlugin.cpp
)

# Set include directories
target_include_directories(plugin_mqtt
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(plugin_mqtt
  PUBLIC
    parameter
    core_insert
    PahoMqttCpp::paho-mqttpp3-static
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()