# rocksdb
if(${BUILD_WITH_ROCKSDB})
    add_subdirectory(rocksdb)
endif(${BUILD_WITH_ROCKSDB})

if(${BUILD_WITH_LUCENE})
    add_subdirectory(lucene)
endif(${BUILD_WITH_LUCENE})

#if(${BUILD_WITH_BDB})
    #add_subdirectory(bdb)
#endif(${BUILD_WITH_BDB})

if(${BUILD_WITH_SQLITE})
    add_subdirectory(sqlite)
endif(${BUILD_WITH_SQLITE})

if(${BUILD_WITH_CRAFT})
    add_subdirectory(craft)
endif(${BUILD_WITH_CRAFT})

if(${BUILD_WITH_TRAFT})
    # add_subdirectory(traft)
endif(${BUILD_WITH_TRAFT})

add_subdirectory(tdev)
