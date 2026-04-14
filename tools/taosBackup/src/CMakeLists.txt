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

# AVRO compatibility module
ADD_SUBDIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/../compatible
                 ${CMAKE_CURRENT_BINARY_DIR}/compatible)

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
        storage/storageParquet.c
        storage/parquetBlock.cpp
        storage/blockReader.c
        storage/colCompress.c
    )

# Enable C++ for the mixed C/C++ target
SET_TARGET_PROPERTIES(taosBackup PROPERTIES LINKER_LANGUAGE CXX)

# C++ standard required by Arrow C++ headers
target_compile_features(taosBackup PRIVATE cxx_std_17)

# Arrow include directory (set by INIT_EXT in external.cmake)
target_include_directories(taosBackup PRIVATE "${ext_arrow_inc_dir}")

# Declare build-time dependency so ext_arrow is compiled before taosBackup links
add_dependencies(taosBackup ext_arrow)

if(NOT ${TD_WINDOWS})
    # Put each function/data in its own section so --gc-sections can dead-strip
    # unused code that is dragged in from Arrow/Parquet static libraries.
    target_compile_options(taosBackup PRIVATE -ffunction-sections -fdata-sections)

    # Prevent ZSTD symbol interposition: libtaos.so exports TSZ's ZSTD symbols
    # which collide with ext_arrow's statically bundled ZSTD.
    if(NOT APPLE)
        target_link_options(taosBackup PRIVATE
            -Wl,--gc-sections
            -Wl,-Bsymbolic-functions
            -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/zstd_local.map
        )
    endif()

    # Link-Time Optimization for non-Debug builds.
    # Works together with -Os (Arrow) and --gc-sections to eliminate dead code
    # at a finer granularity than section-level stripping alone.
    if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
        include(CheckIPOSupported)
        check_ipo_supported(RESULT _ipo_ok OUTPUT _ipo_err LANGUAGES C CXX)
        if(_ipo_ok)
            set_target_properties(taosBackup PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
            message(STATUS "taosBackup: LTO enabled")
        else()
            message(STATUS "taosBackup: LTO not available (${_ipo_err}), skipping")
        endif()
    endif()
endif()

if(TD_WINDOWS)
    # Arrow's Windows headers use __declspec(dllimport) by default.
    # These macros switch to the static-library ABI (no dllimport decoration),
    # which is mandatory when linking against .lib static archives.
    # See: https://arrow.apache.org/docs/developers/cpp/windows.html#statically-linking-to-arrow-on-windows
    target_compile_definitions(taosBackup PRIVATE ARROW_STATIC PARQUET_STATIC)

    # MSVC size-reduction equivalents of the Linux -ffunction-sections /
    # -fdata-sections / --gc-sections / -Os chain:
    #   /Gy  - function-level linking: each function in its own COMDAT so the
    #          linker's /OPT:REF can discard unreferenced ones
    #   /Gw  - each global variable in its own COMDAT (same, for data)
    target_compile_options(taosBackup PRIVATE /Gy /Gw)

    # /OPT:REF - remove unreferenced COMDATs (dead-code stripping)
    # /OPT:ICF - merge identical COMDATs (size deduplication)
    target_link_options(taosBackup PRIVATE /OPT:REF /OPT:ICF)

    # Link-Time Code Generation for non-Debug builds.
    # /GL (compile) + /LTCG (link) = whole-program optimisation, the MSVC
    # equivalent of GCC/Clang -flto.  Dead-strips and inlines across TUs.
    if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
        target_compile_options(taosBackup PRIVATE /GL)
        target_link_options(taosBackup PRIVATE /LTCG)
    endif()
endif()

# link taos
TARGET_LINK_LIBRARIES(taosBackup PRIVATE taos)

# link AVRO compatibility module
TARGET_LINK_LIBRARIES(taosBackup PRIVATE compatAvro)
target_include_directories(taosBackup PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../compatible)
target_compile_definitions(taosBackup PRIVATE COMPAT_AVRO_ENABLED)

# libavro.a (via compatAvro) uses snappy for codec support
DEP_ext_snappy(taosBackup)

if(TD_WINDOWS)
    # Windows: link standard system libraries needed by TDengine client
    target_link_libraries(taosBackup PRIVATE ws2_32 advapi32)

    # Transitive dependencies (e.g. from compatAvro) may reference "lzma.lib"
    # as a bare library name.  Add ext_xz's install directory so the MSVC
    # linker can resolve it.
    if(ext_xz_libs)
        list(GET ext_xz_libs 0 _xz_first_lib)
        get_filename_component(_xz_lib_dir "${_xz_first_lib}" DIRECTORY)
        target_link_directories(taosBackup PRIVATE "${_xz_lib_dir}")
    endif()
endif()

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

# Strip DWARF debug sections after linking to shrink the installed binary.
# GNU strip uses --strip-debug; Apple strip uses -S for the same effect.
if(NOT ${TD_WINDOWS})
    if(APPLE)
        set(_strip_flag -S)
    else()
        set(_strip_flag --strip-debug)
    endif()
    add_custom_command(TARGET taosBackup POST_BUILD
        COMMAND ${CMAKE_STRIP} ${_strip_flag} $<TARGET_FILE:taosBackup>
        COMMENT "Stripping debug sections from taosBackup"
    )
endif()

MESSAGE("")
MESSAGE("********** End build taosBackup ! **********")
MESSAGE("")