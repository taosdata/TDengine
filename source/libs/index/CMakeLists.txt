aux_source_directory(src INDEX_SRC)
add_library(index STATIC ${INDEX_SRC})
target_include_directories(
    index
    PUBLIC "${TD_SOURCE_DIR}/include/libs/index"
    PUBLIC "${TD_SOURCE_DIR}/include/os"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    
)
target_link_libraries(
    index 
    PUBLIC os
    PUBLIC util
    PUBLIC common
    PUBLIC nodes
    PUBLIC scalar 
    PUBLIC function 
)

if (${BUILD_WITH_LUCENE})
  target_include_directories(
    index
    PUBLIC "${TD_SOURCE_DIR}/deps/lucene/include"
  )
  LINK_DIRECTORIES("${TD_SOURCE_DIR}/deps/lucene/debug/src/core")
  target_link_libraries(
    index
    PUBLIC lucene++
  )
endif(${BUILD_WITH_LUCENE})

if (${BUILD_WITH_INVERTEDINDEX})
  add_definitions(-DUSE_INVERTED_INDEX) 
endif(${BUILD_WITH_INVERTEDINDEX})


if (${BUILD_TEST})
   add_subdirectory(test)
endif(${BUILD_TEST})

