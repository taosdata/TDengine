# ================================================================================================
# Download
# ================================================================================================
function(cat IN_FILE OUT_FILE)
    file(READ ${IN_FILE} CONTENTS)
    file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

set(CONTRIB_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
configure_file("${CMAKE_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# pthread
if(${BUILD_PTHREAD})
    cat("${CMAKE_SUPPORT_DIR}/pthread_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# gnu regex
if(${BUILD_GNUREGEX})
    cat("${CMAKE_SUPPORT_DIR}/gnuregex_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# googletest
if(${BUILD_TEST})
    cat("${CMAKE_SUPPORT_DIR}/gtest_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${CMAKE_SUPPORT_DIR}/stub_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_TEST})

# lz4
cat("${CMAKE_SUPPORT_DIR}/lz4_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# zlib
cat("${CMAKE_SUPPORT_DIR}/zlib_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# cJson
cat("${CMAKE_SUPPORT_DIR}/cjson_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# leveldb
if(${BUILD_WITH_LEVELDB})
    cat("${CMAKE_SUPPORT_DIR}/leveldb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_LEVELDB})

# rocksdb
if(${BUILD_WITH_ROCKSDB})
    cat("${CMAKE_SUPPORT_DIR}/rocksdb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_ROCKSDB)
endif(${BUILD_WITH_ROCKSDB})

# canonical-raft
if(${BUILD_WITH_CRAFT})
    cat("${CMAKE_SUPPORT_DIR}/craft_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    SET(BUILD_WITH_UV ON CACHE BOOL "craft need libuv" FORCE)
endif(${BUILD_WITH_CRAFT})

# traft
if(${BUILD_WITH_TRAFT})
    cat("${CMAKE_SUPPORT_DIR}/traft_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    SET(BUILD_WITH_UV ON CACHE BOOL "traft need libuv" FORCE)
endif(${BUILD_WITH_TRAFT})

#libuv
if(${BUILD_WITH_UV})
    cat("${CMAKE_SUPPORT_DIR}/libuv_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_UV})

# bdb
#if(${BUILD_WITH_BDB})
    #cat("${CMAKE_SUPPORT_DIR}/bdb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
#endif(${BUILD_WITH_BDB})

# sqlite
if(${BUILD_WITH_SQLITE})
    cat("${CMAKE_SUPPORT_DIR}/sqlite_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_SQLITE})

# lucene
if(${BUILD_WITH_LUCENE})
    cat("${CMAKE_SUPPORT_DIR}/lucene_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_LUCENE)
endif(${BUILD_WITH_LUCENE})

# NuRaft
if(${BUILD_WITH_NURAFT})
    cat("${CMAKE_SUPPORT_DIR}/nuraft_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_NURAFT})

# download dependencies
configure_file(${CONTRIB_TMP_FILE} "${CMAKE_CONTRIB_DIR}/deps-download/CMakeLists.txt")
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${CMAKE_CONTRIB_DIR}/deps-download")
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_CONTRIB_DIR}/deps-download")

# ================================================================================================
# Build
# ================================================================================================
# googletest
if(${BUILD_TEST})
    add_subdirectory(googletest)
    target_include_directories(
        gtest
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src>
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_linux>
    )
endif(${BUILD_TEST})

# cJson
# see https://stackoverflow.com/questions/37582508/silence-cmp0048-warnings-in-vendored-projects
set(CMAKE_PROJECT_INCLUDE_BEFORE "${CMAKE_SUPPORT_DIR}/EnableCMP0048.txt.in")
option(ENABLE_CJSON_TEST "Enable building cJSON test" OFF)
option(CJSON_OVERRIDE_BUILD_SHARED_LIBS "Override BUILD_SHARED_LIBS with CJSON_BUILD_SHARED_LIBS" ON)
option(CJSON_BUILD_SHARED_LIBS "Overrides BUILD_SHARED_LIBS if CJSON_OVERRIDE_BUILD_SHARED_LIBS is enabled" OFF)
add_subdirectory(cJson)
target_include_directories(
    cjson
    # see https://stackoverflow.com/questions/25676277/cmake-target-include-directories-prints-an-error-when-i-try-to-add-the-source
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cJson>
)
unset(CMAKE_PROJECT_INCLUDE_BEFORE)

# lz4
add_subdirectory(lz4/build/cmake)
target_include_directories(
    lz4_static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lib
)

# zlib
set(CMAKE_PROJECT_INCLUDE_BEFORE "${CMAKE_SUPPORT_DIR}/EnableCMP0048.txt.in")
add_subdirectory(zlib)
target_include_directories(
    zlibstatic
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
)
target_include_directories(
    zlib
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
)
unset(CMAKE_PROJECT_INCLUDE_BEFORE)

# leveldb
if(${BUILD_WITH_LEVELDB})
    option(LEVELDB_BUILD_TESTS "" OFF)
    add_subdirectory(leveldb)
    target_include_directories(
        leveldb
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/leveldb/include>
    )
endif(${BUILD_WITH_LEVELDB})

# rocksdb
# To support rocksdb build on ubuntu: sudo apt-get install libgflags-dev
if(${BUILD_WITH_ROCKSDB})
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=maybe-uninitialized")
    option(WITH_TESTS "" OFF)
    option(WITH_BENCHMARK_TOOLS "" OFF)
    option(WITH_TOOLS "" OFF)
    option(WITH_LIBURING "" OFF)
    option(ROCKSDB_BUILD_SHARED "Build shared versions of the RocksDB libraries" OFF)
    add_subdirectory(rocksdb)
    target_include_directories(
        rocksdb
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include>
    )
endif(${BUILD_WITH_ROCKSDB})

# lucene
# To support build on ubuntu: sudo apt-get install libboost-all-dev
if(${BUILD_WITH_LUCENE})
    option(ENABLE_TEST "Enable the tests" OFF)
    add_subdirectory(lucene)
    target_include_directories(
      lucene++ 
      PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/lucene/include>
    ) 
     
endif(${BUILD_WITH_LUCENE})

# NuRaft
if(${BUILD_WITH_NURAFT})
    add_subdirectory(nuraft)
endif(${BUILD_WITH_NURAFT})

# CRAFT
if(${BUILD_WITH_CRAFT})
    add_library(craft STATIC IMPORTED GLOBAL)
    set_target_properties(craft PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/craft/.libs/libraft.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/craft/include"
    )
    # target_link_libraries(craft
    #     INTERFACE pthread
    # )
endif(${BUILD_WITH_CRAFT})

# TRAFT
if(${BUILD_WITH_TRAFT})
    add_library(traft STATIC IMPORTED GLOBAL)
    set_target_properties(traft PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/traft/.libs/libraft.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/traft/include"
    )
    # target_link_libraries(craft
    #     INTERFACE pthread
    # )
endif(${BUILD_WITH_TRAFT})

# LIBUV
if(${BUILD_WITH_UV})
    if (NOT ${CMAKE_SYSTEM_NAME} MATCHES "Windows")
        MESSAGE("Windows need set no-sign-compare")
        add_compile_options(-Wno-sign-compare)
    endif ()
    add_subdirectory(libuv)
endif(${BUILD_WITH_UV})

# BDB
if(${BUILD_WITH_BDB})
    add_library(bdb STATIC IMPORTED GLOBAL)
    set_target_properties(bdb PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/bdb/libdb.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/bdb"
    )
    target_link_libraries(bdb 
        INTERFACE pthread 
    )
endif(${BUILD_WITH_BDB})

# SQLite
# see https://stackoverflow.com/questions/8774593/cmake-link-to-external-library#comment58570736_10550334
if(${BUILD_WITH_SQLITE})
    add_library(sqlite STATIC IMPORTED GLOBAL)
    set_target_properties(sqlite PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/sqlite/.libs/libsqlite3.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/sqlite"
    )
    target_link_libraries(sqlite 
        INTERFACE m 
        INTERFACE pthread 
        INTERFACE dl
    )
endif(${BUILD_WITH_SQLITE})

# pthread


# ================================================================================================
# Build test
# ================================================================================================
if(${BUILD_DEPENDENCY_TESTS})
    add_subdirectory(test)
endif(${BUILD_DEPENDENCY_TESTS})
