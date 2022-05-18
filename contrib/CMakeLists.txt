# ================================================================================================
# Download
# ================================================================================================
function(cat IN_FILE OUT_FILE)
    file(READ ${IN_FILE} CONTENTS)
    file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

set(CONTRIB_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# pthread
if(${BUILD_PTHREAD})
    cat("${TD_SUPPORT_DIR}/pthread_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# iconv
if(${BUILD_WITH_ICONV})
    cat("${TD_SUPPORT_DIR}/iconv_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# msvc regex
if(${BUILD_MSVCREGEX})
    cat("${TD_SUPPORT_DIR}/msvcregex_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# wcwidth
if(${BUILD_WCWIDTH})
    cat("${TD_SUPPORT_DIR}/wcwidth_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# wingetopt
if(${BUILD_WINGETOPT})
    cat("${TD_SUPPORT_DIR}/wingetopt_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# googletest
if(${BUILD_TEST})
    cat("${TD_SUPPORT_DIR}/gtest_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/stub_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_TEST})

# lz4
cat("${TD_SUPPORT_DIR}/lz4_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# zlib
cat("${TD_SUPPORT_DIR}/zlib_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# cJson
cat("${TD_SUPPORT_DIR}/cjson_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# leveldb
if(${BUILD_WITH_LEVELDB})
    cat("${TD_SUPPORT_DIR}/leveldb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_LEVELDB})

# rocksdb
if(${BUILD_WITH_ROCKSDB})
    cat("${TD_SUPPORT_DIR}/rocksdb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_ROCKSDB)
endif(${BUILD_WITH_ROCKSDB})

# canonical-raft
if(${BUILD_WITH_CRAFT})
    cat("${TD_SUPPORT_DIR}/craft_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    SET(BUILD_WITH_UV ON CACHE BOOL "craft need libuv" FORCE)
endif(${BUILD_WITH_CRAFT})

# traft
if(${BUILD_WITH_TRAFT})
    cat("${TD_SUPPORT_DIR}/traft_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    SET(BUILD_WITH_UV ON CACHE BOOL "traft need libuv" FORCE)
endif(${BUILD_WITH_TRAFT})

#libuv
if(${BUILD_WITH_UV})
    cat("${TD_SUPPORT_DIR}/libuv_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_UV})

# bdb
if(${BUILD_WITH_BDB})
    cat("${TD_SUPPORT_DIR}/bdb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_BDB})

# sqlite
if(${BUILD_WITH_SQLITE})
    cat("${TD_SUPPORT_DIR}/sqlite_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_SQLITE})

# lucene
if(${BUILD_WITH_LUCENE})
    cat("${TD_SUPPORT_DIR}/lucene_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_LUCENE)
endif(${BUILD_WITH_LUCENE})

# NuRaft
if(${BUILD_WITH_NURAFT})
    cat("${TD_SUPPORT_DIR}/nuraft_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_NURAFT})

# addr2line
if(${BUILD_ADDR2LINE})
    cat("${TD_SUPPORT_DIR}/libdwarf_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/addr2line_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_ADDR2LINE})

# download dependencies
configure_file(${CONTRIB_TMP_FILE} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")

# ================================================================================================
# Build
# ================================================================================================
# googletest
if(${BUILD_TEST})
    add_subdirectory(googletest EXCLUDE_FROM_ALL)
    target_include_directories(
        gtest
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src>
    )
    if(${TD_WINDOWS})
        target_include_directories(
            gtest
            PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_win>
        )
    endif(${TD_WINDOWS})
    if(${TD_LINUX})
        target_include_directories(
            gtest
            PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_linux>
        )
    endif(${TD_LINUX})
    if(${TD_DARWIN})
        target_include_directories(
            gtest
            PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_darwin>
        )
    endif(${TD_DARWIN})
    
    
endif(${BUILD_TEST})

# cJson
# see https://stackoverflow.com/questions/37582508/silence-cmp0048-warnings-in-vendored-projects
set(CMAKE_PROJECT_INCLUDE_BEFORE "${TD_SUPPORT_DIR}/EnableCMP0048.txt.in")
option(ENABLE_CJSON_TEST "Enable building cJSON test" OFF)
option(CJSON_OVERRIDE_BUILD_SHARED_LIBS "Override BUILD_SHARED_LIBS with CJSON_BUILD_SHARED_LIBS" ON)
option(CJSON_BUILD_SHARED_LIBS "Overrides BUILD_SHARED_LIBS if CJSON_OVERRIDE_BUILD_SHARED_LIBS is enabled" OFF)
add_subdirectory(cJson EXCLUDE_FROM_ALL)
target_include_directories(
    cjson
    # see https://stackoverflow.com/questions/25676277/cmake-target-include-directories-prints-an-error-when-i-try-to-add-the-source
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cJson>
)
unset(CMAKE_PROJECT_INCLUDE_BEFORE)

# lz4
add_subdirectory(lz4/build/cmake EXCLUDE_FROM_ALL)
target_include_directories(
    lz4_static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lib
)

# zlib
set(CMAKE_PROJECT_INCLUDE_BEFORE "${TD_SUPPORT_DIR}/EnableCMP0048.txt.in")
add_subdirectory(zlib EXCLUDE_FROM_ALL)
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
    add_subdirectory(leveldb EXCLUDE_FROM_ALL)
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
    add_subdirectory(rocksdb EXCLUDE_FROM_ALL)
    target_include_directories(
        rocksdb
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include>
    )
endif(${BUILD_WITH_ROCKSDB})

# lucene
# To support build on ubuntu: sudo apt-get install libboost-all-dev
if(${BUILD_WITH_LUCENE})
    option(ENABLE_TEST "Enable the tests" OFF)
    add_subdirectory(lucene EXCLUDE_FROM_ALL)
    target_include_directories(
      lucene++ 
      PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/lucene/include>
    ) 
     
endif(${BUILD_WITH_LUCENE})

# NuRaft
if(${BUILD_WITH_NURAFT})
    add_subdirectory(nuraft EXCLUDE_FROM_ALL)
endif(${BUILD_WITH_NURAFT})

# pthread
if(${BUILD_PTHREAD})
    set(CMAKE_BUILD_TYPE release)
    add_definitions(-DPTW32_STATIC_LIB)
    add_subdirectory(pthread EXCLUDE_FROM_ALL)
    set_target_properties(libpthreadVC3 PROPERTIES OUTPUT_NAME pthread)
    add_library(pthread INTERFACE)
    target_link_libraries(pthread INTERFACE libpthreadVC3)
endif()

# iconv
if(${BUILD_WITH_ICONV})
    add_library(iconv STATIC iconv/win_iconv.c)
endif(${BUILD_WITH_ICONV})

# wingetopt
if(${BUILD_WINGETOPT})
    add_subdirectory(wingetopt EXCLUDE_FROM_ALL)
endif(${BUILD_WINGETOPT})

# msvcregex
if(${BUILD_MSVCREGEX})
    add_library(msvcregex STATIC "")
    target_sources(msvcregex
        PRIVATE "msvcregex/regex.c"
    )
    target_include_directories(msvcregex
        PRIVATE "msvcregex"
    )
    target_link_libraries(msvcregex 
        INTERFACE Shell32
    )
    SET_TARGET_PROPERTIES(msvcregex PROPERTIES OUTPUT_NAME msvcregex)
endif(${BUILD_MSVCREGEX})

# msvcregex
if(${BUILD_WCWIDTH})
    add_library(wcwidth STATIC "")
    target_sources(wcwidth
        PRIVATE "wcwidth/wcwidth.c"
    )
    target_include_directories(wcwidth
        PRIVATE "wcwidth"
    )
    SET_TARGET_PROPERTIES(wcwidth PROPERTIES OUTPUT_NAME wcwidth)
endif(${BUILD_WCWIDTH})

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
    add_subdirectory(libuv EXCLUDE_FROM_ALL)
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
    )
    if(NOT TD_WINDOWS)
        target_link_libraries(sqlite 
            INTERFACE dl
        )
    endif(NOT TD_WINDOWS)
endif(${BUILD_WITH_SQLITE})

# addr2line
if(${BUILD_ADDR2LINE})
    check_include_file( "sys/types.h"     HAVE_SYS_TYPES_H) 
    check_include_file( "sys/stat.h"      HAVE_SYS_STAT_H ) 
    check_include_file( "inttypes.h"      HAVE_INTTYPES_H   ) 
    check_include_file( "stddef.h"        HAVE_STDDEF_H   ) 
    check_include_file( "stdlib.h"        HAVE_STDLIB_H   ) 
    check_include_file( "string.h"        HAVE_STRING_H   ) 
    check_include_file( "memory.h"        HAVE_MEMORY_H   ) 
    check_include_file( "strings.h"       HAVE_STRINGS_H  ) 
    check_include_file( "stdint.h"        HAVE_STDINT_H   )
    check_include_file( "unistd.h"        HAVE_UNISTD_H   )
    check_include_file( "sgidefs.h"       HAVE_SGIDEFS_H  )
    check_include_file( "stdafx.h"        HAVE_STDAFX_H   )
    check_include_file( "elf.h"           HAVE_ELF_H      ) 
    check_include_file( "libelf.h"        HAVE_LIBELF_H   ) 
    check_include_file( "libelf/libelf.h" HAVE_LIBELF_LIBELF_H) 
    check_include_file( "alloca.h"        HAVE_ALLOCA_H   )
    check_include_file( "elfaccess.h"     HAVE_ELFACCESS_H)
    check_include_file( "sys/elf_386.h"   HAVE_SYS_ELF_386_H  )
    check_include_file( "sys/elf_amd64.h" HAVE_SYS_ELF_AMD64_H)
    check_include_file( "sys/elf_sparc.h" HAVE_SYS_ELF_SPARC_H)
    check_include_file( "sys/ia64/elf.h"  HAVE_SYS_IA64_ELF_H )
    set(VERSION 0.3.1)
    set(PACKAGE_VERSION "\"${VERSION}\"") 
    configure_file(libdwarf/cmake/config.h.cmake config.h)
    file(GLOB_RECURSE LIBDWARF_SOURCES "libdwarf/src/lib/libdwarf/*.c")
    add_library(libdwarf STATIC ${LIBDWARF_SOURCES})
    set_target_properties(libdwarf PROPERTIES OUTPUT_NAME "libdwarf")
    if(HAVE_LIBELF_H OR HAVE_LIBELF_LIBELF_H)
        target_link_libraries(libdwarf PUBLIC libelf)
    endif()
    target_include_directories(libdwarf SYSTEM PUBLIC "libdwarf/src/lib/libdwarf" ${CMAKE_CURRENT_BINARY_DIR})
    file(READ "addr2line/addr2line.c" ADDR2LINE_CONTENT)
    string(REPLACE "static int" "int" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
    string(REPLACE "static void" "void" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
    string(REPLACE "main(" "main_addr2line(" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
    file(WRITE "addr2line/addr2line.c" "${ADDR2LINE_CONTENT}")
    add_library(addr2line STATIC "addr2line/addr2line.c")
    target_link_libraries(addr2line PUBLIC libdwarf dl z)
    target_include_directories(addr2line PUBLIC "libdwarf/src/lib/libdwarf" )
endif(${BUILD_ADDR2LINE})


# ================================================================================================
# Build test
# ================================================================================================
if(${BUILD_DEPENDENCY_TESTS})
    add_subdirectory(test EXCLUDE_FROM_ALL)
endif(${BUILD_DEPENDENCY_TESTS})
