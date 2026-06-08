# CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

MESSAGE("")
MESSAGE("********** Start build taosdump **********")
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
    MESSAGE(STATUS "taosdump commit id: ${GIT_COMMIT_ID}")
    # define
    ADD_DEFINITIONS(-DTAOSDUMP_COMMIT_ID="${GIT_COMMIT_ID}")
ELSE()
    MESSAGE(FATAL_ERROR "build taosdump FIND_PACKAGE(Git) failed.")
ENDIF (GIT_FOUND)


#
# ------------- version --------------
#

MESSAGE("collect --version show info:")
# version: the project-wide cmake variable is BUILD_VER_NUMBER, not TD_VER_NUMBER
IF (DEFINED BUILD_VER_NUMBER)
    ADD_DEFINITIONS(-DTD_VER_NUMBER="${BUILD_VER_NUMBER}")
    MESSAGE(STATUS "version:${BUILD_VER_NUMBER}")
ELSEIF (DEFINED TD_VER_NUMBER)
    ADD_DEFINITIONS(-DTD_VER_NUMBER="${TD_VER_NUMBER}")
    MESSAGE(STATUS "version:${TD_VER_NUMBER}")
ELSE ()
    # abort build
    MESSAGE(FATAL_ERROR "build taosdump not found BUILD_VER_NUMBER define.")
ENDIF ()

#
# ------------- taosdump --------------
#
# Apache Arrow + Parquet are built from source via ExternalProject_Add in
# cmake/external.cmake (ext_arrow), using ARROW_DEPENDENCY_SOURCE=BUNDLED so
# Arrow fetches/builds Thrift, Snappy, Zstd, etc. internally.
# The three static archives produced are:
#   libparquet.a  libarrow.a  libarrow_bundled_dependencies.a

# AVRO compatibility module
ADD_SUBDIRECTORY(compatible)

# include
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
INCLUDE_DIRECTORIES(../inc)

# src
ADD_EXECUTABLE(
        taosdump
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
SET_TARGET_PROPERTIES(taosdump PROPERTIES LINKER_LANGUAGE CXX)

# C++ standard required by Arrow C++ headers
target_compile_features(taosdump PRIVATE cxx_std_17)

# Arrow include directory (set by INIT_EXT in external.cmake)
target_include_directories(taosdump PRIVATE "${ext_arrow_inc_dir}")

# Declare build-time dependency so ext_arrow is compiled before taosdump links
add_dependencies(taosdump ext_arrow)

if(TD_DARWIN)
    # Suppress ld64 duplicate static library noise caused by transitive links.
    include(CheckLinkerFlag)
    check_linker_flag(C "-Wl,-no_warn_duplicate_libraries" _have_no_warn_dup_libs)
    if(_have_no_warn_dup_libs)
        target_link_options(taosdump PRIVATE -Wl,-no_warn_duplicate_libraries)
    endif()
endif()

if(NOT ${TD_WINDOWS})
    # Put each function/data in its own section so --gc-sections can dead-strip
    # unused code that is dragged in from Arrow/Parquet static libraries.
    target_compile_options(taosdump PRIVATE -ffunction-sections -fdata-sections)

    # Prevent ZSTD symbol interposition: libtaos.so exports TSZ's ZSTD symbols
    # which collide with ext_arrow's statically bundled ZSTD.
    if(NOT TD_DARWIN)
        target_link_options(taosdump PRIVATE
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
            set_target_properties(taosdump PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
            message(STATUS "taosdump: LTO enabled")
        else()
            message(STATUS "taosdump: LTO not available (${_ipo_err}), skipping")
        endif()
    endif()
endif()

if(TD_WINDOWS)
    # Arrow's Windows headers use __declspec(dllimport) by default.
    # These macros switch to the static-library ABI (no dllimport decoration),
    # which is mandatory when linking against .lib static archives.
    # See: https://arrow.apache.org/docs/developers/cpp/windows.html#statically-linking-to-arrow-on-windows
    target_compile_definitions(taosdump PRIVATE ARROW_STATIC PARQUET_STATIC)

    # MSVC size-reduction equivalents of the Linux -ffunction-sections /
    # -fdata-sections / --gc-sections / -Os chain:
    #   /Gy  - function-level linking: each function in its own COMDAT so the
    #          linker's /OPT:REF can discard unreferenced ones
    #   /Gw  - each global variable in its own COMDAT (same, for data)
    target_compile_options(taosdump PRIVATE /Gy /Gw)

    # /OPT:REF - remove unreferenced COMDATs (dead-code stripping)
    # /OPT:ICF - merge identical COMDATs (size deduplication)
    target_link_options(taosdump PRIVATE /OPT:REF /OPT:ICF)

    # Link-Time Code Generation for non-Debug builds.
    # /GL (compile) + /LTCG (link) = whole-program optimisation, the MSVC
    # equivalent of GCC/Clang -flto.  Dead-strips and inlines across TUs.
    if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
        target_compile_options(taosdump PRIVATE /GL)
        target_link_options(taosdump PRIVATE /LTCG)
    endif()
endif()

# link taos
TARGET_LINK_LIBRARIES(taosdump PRIVATE taos)

# link AVRO compatibility module
TARGET_LINK_LIBRARIES(taosdump PRIVATE compatAvro)
target_include_directories(taosdump PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/compatible)
target_compile_definitions(taosdump PRIVATE COMPAT_AVRO_ENABLED)

if(TD_WINDOWS)
    # Windows: link standard system libraries needed by TDengine client
    target_link_libraries(taosdump PRIVATE ws2_32 advapi32)

    # Transitive dependencies (e.g. from compatAvro) may reference "lzma.lib"
    # as a bare library name.  Add ext_xz's install directory so the MSVC
    # linker can resolve it.
    if(ext_xz_libs)
        list(GET ext_xz_libs 0 _xz_first_lib)
        get_filename_component(_xz_lib_dir "${_xz_first_lib}" DIRECTORY)
        target_link_directories(taosdump PRIVATE "${_xz_lib_dir}")
    endif()
endif()

# Strip debug symbols from Arrow static libs before linking so the final
# taosdump binary keeps its own DWARF info (file/line in backtraces) but
# does not bloat with Arrow/Parquet/Thrift internals.
if(NOT ${TD_WINDOWS})
    if(TD_DARWIN)
        set(_strip_flag -S)
    else()
        set(_strip_flag --strip-debug)
    endif()
    set(_stripped_arrow_libs "")
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/stripped_arrow")
    foreach(_arrow_lib ${ext_arrow_libs})
        get_filename_component(_arrow_name "${_arrow_lib}" NAME)
        set(_stripped_lib "${CMAKE_CURRENT_BINARY_DIR}/stripped_arrow/${_arrow_name}")
        add_custom_command(
            OUTPUT  "${_stripped_lib}"
            COMMAND ${CMAKE_COMMAND} -E copy "${_arrow_lib}" "${_stripped_lib}"
            COMMAND ${CMAKE_STRIP} ${_strip_flag} "${_stripped_lib}"
            DEPENDS "${_arrow_lib}"
            COMMENT "Stripping debug info from ${_arrow_name}"
        )
        list(APPEND _stripped_arrow_libs "${_stripped_lib}")
    endforeach()
    add_custom_target(strip_arrow_libs ALL DEPENDS ${_stripped_arrow_libs})
    add_dependencies(strip_arrow_libs ext_arrow)
    add_dependencies(taosdump strip_arrow_libs)

    foreach(_stripped_lib ${_stripped_arrow_libs})
        target_link_libraries(taosdump PRIVATE "${_stripped_lib}")
    endforeach()
else()
    # Windows: link Arrow libs as-is (MSVC PDBs are separate files)
    foreach(_arrow_lib ${ext_arrow_libs})
        target_link_libraries(taosdump PRIVATE "${_arrow_lib}")
    endforeach()
endif()

# On macOS, Arrow's bundled thrift / zlib need these system frameworks
if(TD_DARWIN)
    target_link_libraries(taosdump PRIVATE
        "-framework CoreFoundation"
        "-framework Security"
    )
endif()

MESSAGE("")
MESSAGE("********** End build taosdump ! **********")
MESSAGE("")
