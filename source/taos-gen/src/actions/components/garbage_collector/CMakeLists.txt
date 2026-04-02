# Create library target
add_library(components_gc STATIC
  src/GarbageCollector.cpp
)

# Set include directories
target_include_directories(components_gc
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../memory_pool/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../formatter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# Link dependencies
target_link_libraries(components_gc
  PUBLIC
    components_formatter
    components_memory_pool
    utils
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()