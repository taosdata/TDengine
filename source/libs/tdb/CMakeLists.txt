aux_source_directory(src TDB_SRC)
add_library(tdb STATIC ${TDB_SRC})
# target_include_directories(
#     tkv
#     PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/tkv"
#     PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
# )
target_include_directories(
    tdb
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src/inc"
)
target_link_libraries(
    tdb
    PUBLIC os
    PUBLIC util
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
