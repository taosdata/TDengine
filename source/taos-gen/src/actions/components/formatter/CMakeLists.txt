# Create library target
add_library(components_formatter STATIC
  src/FormatterFactory.cpp
  src/StmtV2Data.cpp
  src/PatternGenerator.cpp
  src/KeyGenerator.cpp
  src/TopicGenerator.cpp
  src/RowSerializer.cpp
)

# Set include directories
target_include_directories(components_formatter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../memory_pool/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../compressor/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../encoding/inc
)

# Link dependencies
target_link_libraries(components_formatter
  PUBLIC
    components_memory_pool
    components_compressor
    components_encoding
    actions_config
    utils
    # TAOS::TAOS
    nlohmann_json::nlohmann_json
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()