# Create library target
add_library(workflow STATIC
  src/Job.cpp
)

# Set include directories
target_include_directories(workflow
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
)

# Link dependent libraries
target_link_libraries(workflow
  PUBLIC
    yaml-cpp::yaml-cpp
)

# Enable tests
# if(TSGEN_ENABLE_TEST)
#   add_subdirectory(test)
# endif()