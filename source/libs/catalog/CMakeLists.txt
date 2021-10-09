aux_source_directory(src CATALOG_SRC)
add_library(catalog ${CATALOG_SRC})
target_include_directories(
    catalog
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/catalog"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
   catalog
   PRIVATE os util common transport
)