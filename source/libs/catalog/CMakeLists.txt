aux_source_directory(src CATALOG_SRC)
add_library(catalog STATIC ${CATALOG_SRC})
target_include_directories(
    catalog
    PUBLIC "${TD_SOURCE_DIR}/include/libs/catalog"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    catalog
    PRIVATE os util transport qcom nodes extconnector
)

if(TD_ENTERPRISE AND UNIX AND NOT APPLE)
    # catalog's static link closure pulls in enterprise extconnector, which
    # references nodesRemotePlanToSQL from libnodes.a after libnodes.a may
    # already have been scanned. Force extraction for catalog consumers.
    target_link_options(catalog INTERFACE "LINKER:--undefined=nodesRemotePlanToSQL")
endif()

if(BUILD_TEST AND NOT TD_WINDOWS)
 ADD_SUBDIRECTORY(test)
endif()
