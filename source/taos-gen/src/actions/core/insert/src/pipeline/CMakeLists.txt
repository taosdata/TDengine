# Create library target
add_library(insert_pipeline STATIC
  src/DataPipeline.cpp
)

# Set include directories
target_include_directories(insert_pipeline
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/inc
)

# Link dependent libraries
target_link_libraries(insert_pipeline
  PUBLIC
    actions_config
    utils
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()