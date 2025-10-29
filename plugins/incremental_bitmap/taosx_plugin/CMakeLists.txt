cmake_minimum_required(VERSION 3.16)

# taosX Plugin Interface - Independent build target
# Default disabled, enabled via BUILD_TAOSX_PLUGIN option

# Use the option defined in parent CMakeLists.txt
if(BUILD_TAOSX_PLUGIN)
    message(STATUS "Building taosX plugin interface")
    
    # Set C standard
    set(CMAKE_C_STANDARD 11)
    
    # Plugin source files
    set(TAOSX_PLUGIN_SOURCES
        taosx_plugin_interface.c
    )
    
    # Create shared library for taosX plugin
    add_library(taosx_incremental_bitmap_plugin SHARED ${TAOSX_PLUGIN_SOURCES})
    
    # Set output properties
    set_target_properties(taosx_incremental_bitmap_plugin PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../build
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../build
        VERSION 0.1.0
        SOVERSION 0
    )
    
    # Include directories
    target_include_directories(taosx_incremental_bitmap_plugin
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    )
    
    # Link libraries (minimal dependencies for taosX compatibility)
    target_link_libraries(taosx_incremental_bitmap_plugin
        PRIVATE pthread
    )
    
    # Compiler flags for plugin compatibility
    target_compile_definitions(taosx_incremental_bitmap_plugin
        PRIVATE TAOSX_PLUGIN_BUILD=1
    )
    
    # Installation rules
    install(TARGETS taosx_incremental_bitmap_plugin
        LIBRARY DESTINATION lib/taosx/plugins
        RUNTIME DESTINATION bin
    )
    
    # Create a simple test executable
    add_executable(test_taosx_plugin_interface
        ${CMAKE_CURRENT_SOURCE_DIR}/test_taosx_plugin.c
    )
    
    target_link_libraries(test_taosx_plugin_interface
        PRIVATE taosx_incremental_bitmap_plugin
    )
    
    # Set test output directory
    set_target_properties(test_taosx_plugin_interface PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../build
    )
    
else()
    message(STATUS "taosX plugin interface disabled (use -DBUILD_TAOSX_PLUGIN=ON to enable)")
endif()
