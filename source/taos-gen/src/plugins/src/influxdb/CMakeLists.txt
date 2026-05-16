# Find dependent packages
find_package(CURL REQUIRED)

# Create library target
add_library(plugin_influxdb STATIC
  src/InfluxDBRegistrar.cpp
  src/InfluxDBInsertDataFormatter.cpp
  src/InfluxDBClient.cpp
  src/InfluxDBSinkPlugin.cpp
)

# Set include directories
target_include_directories(plugin_influxdb
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Link dependent libraries
target_link_libraries(plugin_influxdb
  PUBLIC
    parameter
    core_insert
    CURL::libcurl
    ZLIB::ZLIB
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()
