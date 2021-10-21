aux_source_directory(src INDEX_SRC)
add_library(index ${INDEX_SRC})
target_include_directories(
    index
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/index"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    index 
    PUBLIC os
    PUBLIC util
)

if (${BUILD_WITH_LUCENE})
  target_include_directories(
    index
    PUBLIC "${CMAKE_SOURCE_DIR}/deps/lucene/include"
  )
  LINK_DIRECTORIES("${CMAKE_SOURCE_DIR}/deps/lucene/debug/src/core")
  target_link_libraries(
    index
    PUBLIC lucene++
  )

endif(${BUILD_WITH_LUCENE})

if (${BUILD_TEST})
  add_subdirectory(test)
endif(${BUILD_TEST})

