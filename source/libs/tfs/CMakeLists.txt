aux_source_directory(src TFS_SRC)
add_library(tfs STATIC ${TFS_SRC})
target_include_directories(
  tfs
  PUBLIC "${TD_SOURCE_DIR}/include/libs/tfs"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(tfs os util common monitor)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})