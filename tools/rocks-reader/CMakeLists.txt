aux_source_directory(src RREADER_SRC)

add_executable(rocks-reader ${RREADER_SRC} ./rreader.cpp)

if (${BUILD_CONTRIB} OR NOT ${TD_LINUX})
  DEP_ext_rocksdb(rocks-reader)
else()
  if(${TD_LINUX})
        target_include_directories(
            rocks-reader
            PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
        )
        target_link_directories(
            rocks-reader
            PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
        )

	target_link_libraries(rocks-reader
	    PUBLIC rocksdb
	    PUBLIC pthread
	    PUBLIC m
	)
  endif()
endif()
