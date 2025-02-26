# RocksDB.cmake - Configuration for RocksDB dependency
# This file is included from contrib/CMakeLists.txt

# Only build RocksDB if BUILD_CONTRIB is true or if NOT TD_LINUX
# TODO: figure out the following and correct
# if (${BUILD_CONTRIB} OR NOT ${TD_LINUX})
if (TRUE)
    # Set RocksDB build options, common for all platforms
    set(ROCKSDB_CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DPORTABLE=ON
        -DWITH_FALLOCATE=OFF
        -DWITH_JEMALLOC=OFF
        -DWITH_GFLAGS=OFF
        -DWITH_LIBURING=OFF
        -DFAIL_ON_WARNINGS=OFF
        -DWITH_TESTS=OFF
        -DWITH_BENCHMARK_TOOLS=OFF
        -DWITH_TOOLS=OFF
        -DROCKSDB_BUILD_SHARED=OFF
    )

    # Linux platform-specific options
    if(${TD_LINUX})
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL} -Wno-error=maybe-uninitialized -Wno-error=unused-but-set-variable -Wno-error=unused-variable -Wno-error=unused-function -Wno-errno=unused-private-field -Wno-error=unused-result")

        if("${CMAKE_BUILD_TYPE}" STREQUAL "")
            SET(CMAKE_BUILD_TYPE Release)
        endif()
        
        # Pass these flags to RocksDB build (Actually we don't do it like this, we'll pass it directly)
        # list(APPEND ROCKSDB_CMAKE_ARGS
        #     -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS}"
        # )
    endif()    

    # Windows platform-specific options
    if(${TD_WINDOWS})
        # Set Windows-specific compiler flags
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4244 /wd4819 /std:c++17")
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4244 /wd4819")
        
        # Pass these flags to RocksDB build
        list(APPEND ROCKSDB_CMAKE_ARGS
            # -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS}"
            # -DCMAKE_C_FLAGS="${CMAKE_C_FLAGS}"
            -DWITH_JNI=OFF 
        )
        
        # Set runtime library option
        if(CMAKE_C_FLAGS MATCHES "/MT" OR CMAKE_C_FLAGS MATCHES "/MTd")
            message("Rocksdb build runtime lib use /MT or /MTd")
            list(APPEND ROCKSDB_CMAKE_ARGS -DWITH_MD_LIBRARY=OFF)
        endif()
        
        # Set system libraries, as a CMake string
        # TODO: check ${SYSTEM_LIBS} here really cannot be a multi-item list yet
        set(SYSTEM_LIBS "${SYSTEM_LIBS} shlwapi.lib rpcrt4.lib")
        
        # Pass system libraries to RocksDB build, as an element to a CMake list
        list(APPEND ROCKSDB_CMAKE_ARGS
            [[-DSYSTEM_LIBS="${SYSTEM_LIBS}"]]
        )
    endif()

    # MacOS platform-specific options
    if(${TD_DARWIN})
        # Set Darwin-specific compiler flags
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=maybe-uninitialized -Wno-unused-but-set-variable")
        
        # Pass these flags to RocksDB build
        list(APPEND ROCKSDB_CMAKE_ARGS
            # -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS}"
            # ${CMAKE_CXX_FLAGS}
            # -Wno-error=maybe-uninitialized 
            # -Wno-unused-but-set-variable
            -DHAVE_THREAD_LOCAL=OFF
            -DWITH_IOSTATS_CONTEXT=OFF
            -DWITH_PERF_CONTEXT=OFF
        )

        # Special ARM64 options for Mac M processors
        if(${TD_DARWIN_ARM64})
            list(APPEND ROCKSDB_CMAKE_ARGS -DHAS_ARMV8_CRC=ON)
        endif()
    endif()

    # IMPORTANT: let's not do this, the quotation marks will be messed up, let's pass them directly
    # list(APPEND ROCKSDB_CMAKE_ARGS
        # -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS}"
        # -DCMAKE_C_FLAGS="${CMAKE_C_FLAGS}"
    # )

    MESSAGE(STATUS "Adding RocksDB an an external project")

    include(ExternalProject)
    ExternalProject_Add(rocksdb-project
        URL https://github.com/facebook/rocksdb/archive/refs/tags/v8.1.1.tar.gz
        URL_HASH MD5=3b4c97ee45df9c8a5517308d31ab008b
        DOWNLOAD_NO_PROGRESS 1
        DOWNLOAD_DIR "${TD_CONTRIB_DIR}/deps-download"
        SOURCE_DIR "${TD_CONTRIB_DIR}/rocksdb"
        CMAKE_ARGS ${ROCKSDB_CMAKE_ARGS}
        CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}
        CMAKE_C_FLAGS ${CMAKE_C_FLAGS}
        BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel 4
        BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/build/lib/librocksdb.a
        GIT_SHALLOW true
    )

    # Before setting up the imported target, make sure the directory exists
    file(MAKE_DIRECTORY "${TD_CONTRIB_DIR}/rocksdb/include")
    # TODO: might we need the following?
    # file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/build/include")

    # Create imported target for RocksDB
    add_library(rocksdb STATIC IMPORTED GLOBAL)
    set_target_properties(rocksdb PROPERTIES
        IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/build/lib/librocksdb.a"
        INTERFACE_INCLUDE_DIRECTORIES "${TD_CONTRIB_DIR}/rocksdb/include"
    )
    add_dependencies(rocksdb rocksdb-project)
    
    # Add definition for RocksDB
    add_definitions(-DUSE_ROCKSDB)
endif()
