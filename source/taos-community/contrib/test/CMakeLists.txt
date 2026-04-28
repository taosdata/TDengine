# rocksdb
if(TD_USE_ROCKSDB)
    add_subdirectory(rocksdb)
endif()

# cos
#if(BUILD_WITH_COS)
#    add_subdirectory(cos)
#endif()

if(BUILD_WITH_LUCENE)
    add_subdirectory(lucene)
endif()

if(BUILD_WITH_BDB)
    add_subdirectory(bdb)
endif()

if(BUILD_WITH_SQLITE)
    add_subdirectory(sqlite)
endif()

# if(${BUILD_SHARED_STORAGE})
#   add_subdirectory(azure)
# endif()

add_subdirectory(tdev)
add_subdirectory(lz4)
