# Include directories
include_directories(
  ${CMAKE_SOURCE_DIR}/inc
)

# Add subdirectories (in dependency order)
add_subdirectory(utils)
add_subdirectory(actions)
add_subdirectory(parameter)
add_subdirectory(workflow)
add_subdirectory(engine)
add_subdirectory(plugins)

# Main executable
add_executable(taosgen benchmark.cpp)

# Set include directories
target_include_directories(taosgen
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/parameter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/engine/inc
)

# Link all dependent libraries
target_link_libraries(taosgen
  PRIVATE
    actions
    plugins
    engine
    workflow
    parameter
    utils
    yaml-cpp::yaml-cpp
    luajit::luajit
    # TAOS::TAOS
    Threads::Threads
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_libraries(taosgen PRIVATE -static-libstdc++ -static-libgcc ${ALLOCATOR_LIB})
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_libraries(taosgen PRIVATE ${ALLOCATOR_LIB})
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
  target_link_libraries(taosgen PRIVATE ${ALLOCATOR_LIB} psapi)
endif()

# Install target
install(TARGETS taosgen
  RUNTIME DESTINATION bin
)

if(TSGEN_LINUX OR TSGEN_DARWIN)
  set_target_properties(taosgen PROPERTIES
    INSTALL_RPATH "/usr/local/lib"
  )
endif()