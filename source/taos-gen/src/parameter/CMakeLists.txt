# Create library target
add_library(parameter STATIC
  src/ParameterContext.cpp
  src/StepParserRegistry.cpp
)

target_compile_definitions(parameter PRIVATE
  TSGEN_BUILD_TARGET_OSTYPE="${TSGEN_BUILD_TARGET_OSTYPE}"
  TSGEN_BUILD_TARGET_CPUTYPE="${TSGEN_BUILD_TARGET_CPUTYPE}"
  TSGEN_BUILD_DATE="${TSGEN_BUILD_DATE}"
  TSGEN_BUILD_GIT="${TSGEN_BUILD_GIT}"
)

# Set include directories
target_include_directories(parameter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/core/checkpoint/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/base
    ${PROJECT_SOURCE_DIR}/src
)

# Link dependent libraries
target_link_libraries(parameter
  PUBLIC
    actions_config
    core_checkpoint
    yaml-cpp::yaml-cpp
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()