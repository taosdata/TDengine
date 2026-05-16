# Create library target
add_library(components_compressor STATIC
  src/CompressionType.cpp
  src/Compressor.cpp
  src/GzipCompressor.cpp
  src/Lz4Compressor.cpp
  src/ZstdCompressor.cpp
)

# Set include directories
target_include_directories(components_compressor
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# Link dependencies
target_link_libraries(components_compressor
  PUBLIC
    ZLIB::ZLIB
    lz4::lz4
    zstd::libzstd_static
)

# Set compilation options
# target_compile_options(components_compressor PRIVATE
#     -fno-strict-aliasing
# )

# Set link options
# set_target_properties(components_compressor PROPERTIES
#     LINK_FLAGS "-Wl,--export-dynamic"
# )

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()