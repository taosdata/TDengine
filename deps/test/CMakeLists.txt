# rocksdb
if(${BUILD_WITH_ROCKSDB})
    add_subdirectory(rocksdb)
endif(${BUILD_WITH_ROCKSDB})

if(${BUILD_WITH_LUCENE})
    add_subdirectory(lucene)
endif(${BUILD_WITH_LUCENE})
