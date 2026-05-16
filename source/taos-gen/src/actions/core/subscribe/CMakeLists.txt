# Add submodules

# Create library target
add_library(core_subscribe STATIC
  src/SubscribeDataAction.cpp
)

# Set include directories
target_include_directories(core_subscribe
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../base
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
)

# Link dependent libraries
target_link_libraries(core_subscribe
  PUBLIC
    actions_config
    utils
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()