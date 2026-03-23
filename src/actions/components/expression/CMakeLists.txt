# Create library target
add_library(components_expression STATIC
  src/MathFunctions.cpp
  src/NetworkFunctions.cpp
  src/ExpressionEngine.cpp
)

# Set include directories
target_include_directories(components_expression
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# Link dependencies
target_link_libraries(components_expression
  PUBLIC
    actions_config
    luajit::luajit
    Threads::Threads
)

if(TSGEN_LINUX)
  target_link_libraries(components_expression PUBLIC m dl)
endif()

# Set compilation options
if(TSGEN_WINDOWS)
  # MSVC does not support -fno-strict-aliasing
else()
  target_compile_options(components_expression PRIVATE
      -fno-strict-aliasing
  )
endif()

# Set link options
if(TSGEN_LINUX)
  set_target_properties(components_expression PROPERTIES
      LINK_FLAGS "-Wl,--export-dynamic"
  )
endif()

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()
