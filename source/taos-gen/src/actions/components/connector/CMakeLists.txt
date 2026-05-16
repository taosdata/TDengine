# Create library target
add_library(components_connector STATIC
  src/ConnectionPoolImpl.cpp
  src/ConnectorFactory.cpp
  src/ConnectorSource.cpp
  src/PooledConnector.cpp
  src/TDengineConnector.cpp
  src/RestfulConnector.cpp
)

# Set include directories
target_include_directories(components_connector
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../formatter/inc
)

# Link dependencies
target_link_libraries(components_connector
  PUBLIC
    components_formatter
    actions_config
    # TAOS::TAOS
)

if(UNIX AND NOT APPLE)
  target_link_libraries(components_connector PUBLIC dl)
endif()

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()