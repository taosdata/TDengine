aux_source_directory(src BLOB_SRC)
add_library(blob STATIC ${BLOB_SRC})

target_include_directories(
  blob
  PUBLIC "${TD_SOURCE_DIR}/include/libs/blob"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
  blob
  PUBLIC os util common
)

if(${BUILD_TEST})
  ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
