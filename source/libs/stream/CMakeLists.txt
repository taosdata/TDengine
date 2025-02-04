aux_source_directory(src STREAM_SRC)
add_library(stream STATIC ${STREAM_SRC})
target_include_directories(
  stream
  PUBLIC "${TD_SOURCE_DIR}/include/libs/stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/tcs"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(${FALLBACK})                    # {
  if(${BUILD_WITH_ROCKSDB})
    if(${BUILD_CONTRIB})
      target_link_libraries(
        stream
        PUBLIC rocksdb tdb tcs
        PRIVATE os util transport qcom executor wal index
      )
      target_include_directories(
        stream
        PUBLIC "${TD_SOURCE_DIR}/contrib/rocksdb/include"
      )
    else()
      if(TD_LINUX)
        target_include_directories(
          stream
          PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
        )
        target_link_directories(
          stream
          PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
        )
        target_link_libraries(
          stream
          PUBLIC rocksdb tdb tcs
          PRIVATE os util transport qcom executor wal index
        )
      else()
        target_link_libraries(
          stream
          PUBLIC rocksdb tdb tcs
          PRIVATE os util transport qcom executor wal index
        )
        target_include_directories(
          stream
          PUBLIC "${TD_SOURCE_DIR}/contrib/rocksdb/include"
        )
      endif()
    endif()

    add_definitions(-DUSE_ROCKSDB)
  endif(${BUILD_WITH_ROCKSDB})
else()                             # }{
    if(${BUILD_WITH_ROCKSDB} AND (${BUILD_CONTRIB} OR TD_LINUX))
        # freemine: check the original weird logic above
        DEP_ext_rocksdb(stream)
        if(${TD_WINDOWS})
            target_link_libraries(stream PUBLIC Rpcrt4 Shlwapi)
        else()
            target_link_libraries(stream PUBLIC m stdc++)
        endif()
        add_definitions(-DUSE_ROCKSDB)
    endif()

    target_link_libraries(
        stream
        PUBLIC tdb tcs
        PRIVATE os util transport qcom executor wal index
    )
endif()                            # }

# target_link_libraries(
# stream
# PUBLIC tdb
# PRIVATE os util transport qcom executor
# )
if(${BUILD_TEST})
  ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
