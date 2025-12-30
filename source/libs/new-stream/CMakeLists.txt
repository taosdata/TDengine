aux_source_directory(src STREAM_SRC)
add_library(new-stream STATIC ${STREAM_SRC})

target_include_directories(
    new-stream
    PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
    PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
if (${BUILD_CONTRIB} OR NOT ${TD_LINUX})
  DEP_ext_rocksdb(new-stream)
else()
  target_link_libraries(new-stream PRIVATE rocksdb)
endif()

if(${BUILD_WITH_ROCKSDB})
  if(TD_LINUX)
    target_include_directories(
      new-stream
      PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
    )
    target_link_directories(
      new-stream
      PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
    )
  endif()
  add_definitions(-DUSE_ROCKSDB)
endif(${BUILD_WITH_ROCKSDB})

target_link_libraries(
    new-stream
    PUBLIC tdb
    PRIVATE os util transport qcom executor wal index
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
