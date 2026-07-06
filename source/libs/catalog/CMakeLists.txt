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
    PRIVATE os util transport qcom nodes
)

if(BUILD_TEST AND NOT TD_WINDOWS)
 target_compile_definitions(catalog PRIVATE BUILD_TEST)
 ADD_SUBDIRECTORY(test)
endif()
