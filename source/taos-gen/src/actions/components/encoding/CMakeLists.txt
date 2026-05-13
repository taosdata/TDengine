# Create library target
add_library(components_encoding STATIC
  src/EncodingType.cpp
  src/EncodingConverter.cpp
)

# Set include directories
target_include_directories(components_encoding
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# Link dependencies
target_link_libraries(components_encoding
  PUBLIC
    Iconv::Iconv
)

# Set compilation options
# target_compile_options(components_encoding PRIVATE
#     -fno-strict-aliasing
# )

# Set link options
# set_target_properties(components_encoding PROPERTIES
#     LINK_FLAGS "-Wl,--export-dynamic"
# )

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()