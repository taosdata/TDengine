# rocksdb
if(${BUILD_WITH_ROCKSDB})
    add_subdirectory(rocksdb)
endif(${BUILD_WITH_ROCKSDB})

# cos
#if(${BUILD_WITH_COS})
#    add_subdirectory(cos)
#endif(${BUILD_WITH_COS})

if(${BUILD_WITH_LUCENE})
    add_subdirectory(lucene)
endif(${BUILD_WITH_LUCENE})

if(${BUILD_WITH_BDB})
    add_subdirectory(bdb)
endif(${BUILD_WITH_BDB})

if(${BUILD_WITH_SQLITE})
    add_subdirectory(sqlite)
endif(${BUILD_WITH_SQLITE})

# if(${BUILD_SHARED_STORAGE})
#   add_subdirectory(azure)
# endif()

add_subdirectory(tdev)
add_subdirectory(lz4)
