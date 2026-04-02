# Create library target
add_library(components_generator STATIC
  src/TableNameGenerator.cpp
  src/TimestampGenerator.cpp
  src/ColumnGeneratorFactory.cpp
  src/RandomColumnGenerator.cpp
  src/OrderColumnGenerator.cpp
  src/ExprColumnGenerator.cpp
  src/RowGenerator.cpp
)

# Set include directories
target_include_directories(components_generator
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../expression/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# Link dependencies
target_link_libraries(components_generator
  PUBLIC
    components_expression
    actions_config
    utils
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()