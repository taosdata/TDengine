# Add subdirectories
add_subdirectory(csv)
# add_subdirectory(kafka)

# Create library target
add_library(components_reader STATIC 
  $<TARGET_OBJECTS:reader_csv>
)

# Set include directories
target_include_directories(components_reader
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/csv/inc
)

# Link submodules
target_link_libraries(components_reader
  PUBLIC
    reader_csv
)