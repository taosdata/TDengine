aux_source_directory(src CATALOG_SRC)
add_library(catalog STATIC ${CATALOG_SRC})
target_include_directories(
    catalog
    PUBLIC "${TD_SOURCE_DIR}/include/libs/catalog"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(BUILD_TEST)
    target_compile_definitions(catalog PRIVATE BUILD_TEST)
endif()

target_link_libraries(
    catalog
    PRIVATE os util transport qcom nodes extconnector
)

if(UNIX AND NOT APPLE)
    # catalog participates in static-library cycles through parser/planner/executor.
    # Ask CMake to rescan the link interface enough times for GNU ld.
    set_property(TARGET catalog PROPERTY LINK_INTERFACE_MULTIPLICITY 3)
endif()

if(TD_ENTERPRISE AND UNIX AND NOT APPLE)
    # catalog's static link closure pulls in enterprise extconnector, which
    # references nodesRemotePlanToSQL from libnodes.a after libnodes.a may
    # already have been scanned. Force extraction for catalog consumers.
    target_link_options(catalog INTERFACE "LINKER:--undefined=nodesRemotePlanToSQL")
endif()

if(BUILD_TEST AND NOT TD_WINDOWS)
 target_compile_definitions(catalog PRIVATE BUILD_TEST)
 ADD_SUBDIRECTORY(test)
endif()
