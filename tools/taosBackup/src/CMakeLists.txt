# CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

MESSAGE("")
MESSAGE("********** Start build taosBackup **********")
MESSAGE("")

#
# ------------- buildinfo --------------
#
MESSAGE(STATUS "The current platform " ${CMAKE_SYSTEM_PROCESSOR} " is detected")

IF (CMAKE_SYSTEM_PROCESSOR MATCHES "(amd64)|(AMD64)|(x86_64)")
    MESSAGE(STATUS "The current platform is amd64")
    MESSAGE(STATUS "Set CPUTYPE to x64")
    SET(CURRENT_CPUTYPE "x64")
    SET(CURRENT_PLATFORM_ARCH_STR "amd64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)")
    MESSAGE(STATUS "The current platform is x86")
    MESSAGE(STATUS "Set CPUTYPE to x86")
    SET(CURRENT_CPUTYPE "x32")
    SET(CURRENT_PLATFORM_ARCH_STR "i386")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "armv7l")
    MESSAGE(STATUS "Set CPUTYPE to aarch32")
    SET(CURRENT_CPUTYPE "aarch32")
    MESSAGE(STATUS "Set CPUTYPE to aarch32")
    SET(CURRENT_PLATFORM_ARCH_STR "arm")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    SET(CURRENT_CPUTYPE "aarch64")
    MESSAGE(STATUS "Set CPUTYPE to aarch64")
    SET(CURRENT_PLATFORM_ARCH_STR "arm64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "mips64")
    SET(CURRENT_CPUTYPE "mips64")
    MESSAGE(STATUS "Set CPUTYPE to mips64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
    SET(CURRENT_CPUTYPE "apple_m1")
    MESSAGE(STATUS "Set CPUTYPE to apple silicon m1")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "loongarch64")
    SET(CURRENT_CPUTYPE "loongarch64")
    MESSAGE(STATUS "Set CPUTYPE to loongarch64")
ENDIF ()

# build info
SET(BUILD_INFO "${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}")
ADD_DEFINITIONS(-DBUILD_INFO="${BUILD_INFO}")
MESSAGE(STATUS "build:${BUILD_INFO}")
MESSAGE("")

# commit id
FIND_PACKAGE(Git)
IF(GIT_FOUND)
    # get
    EXECUTE_PROCESS(
        COMMAND git log -1 --format=%H
        WORKING_DIRECTORY ${TD_COMMUNITY_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_ID
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )    
    # show
    MESSAGE(STATUS "taosBackup commit id: ${GIT_COMMIT_ID}")
    # define
    ADD_DEFINITIONS(-DTAOSBACKUP_COMMIT_ID="${GIT_COMMIT_ID}")
ELSE()
    MESSAGE(FATAL_ERROR "build taosBackup FIND_PACKAGE(Git) failed.")
ENDIF (GIT_FOUND)


#
# ------------- version --------------
#

MESSAGE("collect --version show info:")
# version
IF (DEFINED TD_VER_NUMBER)
    ADD_DEFINITIONS(-DTD_VER_NUMBER="${TD_VER_NUMBER}")
    MESSAGE(STATUS "version:${TD_VER_NUMBER}")
ELSE ()    
    # abort build
    MESSAGE(FATAL_ERROR "build taosBackup not found TD_VER_NUMBER define.")
ENDIF ()

#
# ------------- taosBackup --------------
#
# Apache Arrow + Parquet are built from source via ExternalProject_Add in
# cmake/external.cmake (ext_arrow), using ARROW_DEPENDENCY_SOURCE=BUNDLED so
# Arrow fetches/builds Thrift, Snappy, Zstd, etc. internally.
# The three static archives produced are:
#   libparquet.a  libarrow.a  libarrow_bundled_dependencies.a

# include
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
INCLUDE_DIRECTORIES(../inc)

# src
ADD_EXECUTABLE(
        taosBackup
        main.c
        # core
        core/bckUtil.c
        core/bckArgs.c
        core/bckLog.c
        core/bckFile.c
        core/bckDb.c
        core/bckPool.c
        core/bckProgress.c
        # backup
        backup/backup.c
        backup/backupMeta.c
        backup/backupData.c
        # restore
        restore/restore.c
        restore/restoreMeta.c
        restore/restoreData.c
        restore/restoreCkpt.c
        restore/restoreStmt.c
        restore/restoreStmt2.c
        restore/bckSchemaChange.c
        # storage
        storage/storageTaos.c
    )

if(NOT ${TD_WINDOWS})
    # Arrow/Parquet storage backend (Linux and macOS)
    target_sources(taosBackup PRIVATE
        storage/storageParquet.c
        storage/parquetBlock.cpp
        storage/blockReader.c
        storage/colCompress.c
    )
endif()

# Enable C++ for the mixed C/C++ target
SET_TARGET_PROPERTIES(taosBackup PROPERTIES LINKER_LANGUAGE CXX)

if(NOT ${TD_WINDOWS})
# Put each function/data in its own section so --gc-sections can dead-strip
# unused code that is dragged in from Arrow/Parquet static libraries.
target_compile_options(taosBackup PRIVATE -ffunction-sections -fdata-sections)

# Code coverage instrumentation
if(ENABLE_COVERAGE)
    target_compile_options(taosBackup PRIVATE --coverage)
    target_link_options(taosBackup PRIVATE --coverage)
endif()

# Prevent ZSTD symbol interposition: libtaos.so exports TSZ's ZSTD symbols
# (ZSTD_compressBlock_doubleFast etc.) which collide with ext_arrow's statically
# bundled ZSTD.  With a PIE binary, PLT lookup can resolve internal ZSTD calls
# to libtaos.so's copy, causing a NULL-context crash.
# The version script marks all ZSTD_* symbols as "local" in the binary so they
# are never preemptable by the shared library's copy.
if(NOT APPLE)
    target_link_options(taosBackup PRIVATE
        -Wl,--gc-sections
        -Wl,-Bsymbolic-functions
        -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/zstd_local.map
    )
endif()

# C++ standard required by Arrow C++ headers
target_compile_features(taosBackup PRIVATE cxx_std_17)

# Arrow include directory (set by INIT_EXT in external.cmake)
target_include_directories(taosBackup PRIVATE "${ext_arrow_inc_dir}")

# Declare build-time dependency so ext_arrow is compiled before taosBackup links
add_dependencies(taosBackup ext_arrow)
endif()

# link taos
TARGET_LINK_LIBRARIES(taosBackup PRIVATE taos)

if(NOT ${TD_WINDOWS})
# link Arrow static libs: parquet → arrow → bundled deps (order matters for linker)
foreach(_arrow_lib ${ext_arrow_libs})
    target_link_libraries(taosBackup PRIVATE "${_arrow_lib}")
endforeach()

# On macOS, Arrow's bundled thrift / zlib need these system frameworks
if(APPLE)
    target_link_libraries(taosBackup PRIVATE
        "-framework CoreFoundation"
        "-framework Security"
    )
endif()
endif()

MESSAGE("")
MESSAGE("********** End build taosBackup ! **********")
MESSAGE("")