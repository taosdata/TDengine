# ================================================================================================
# Download
# ================================================================================================

if(${BUILD_WITH_S3})
    # file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.2/)

elseif(${BUILD_WITH_COS})
    message(FATAL_ERROR "freemine: not implemented yet")
endif()

# s3
if(${BUILD_WITH_S3})
    add_definitions(-DUSE_S3)

# cos
elseif(${BUILD_WITH_COS})
    message(FATAL_ERROR "freemine: not implemented yet")
    add_definitions(-DUSE_COS)
endif()

# taosadapter
if(${BUILD_HTTP})
    MESSAGE("BUILD_HTTP is on")
else()
    MESSAGE("BUILD_HTTP is off, use taosAdapter")
endif()

# lemon
if(${BUILD_WITH_LEMON})
    if(${TD_ASTRA})
        set(C_COMPILER_LEMON ${CMAKE_C_COMPILER})
        if(C_COMPILER_LEMON)
            message(STATUS "LEMON C compiler: ${C_COMPILER_LEMON}")
        else()
            set(C_COMPILER_LEMON ${CMAKE_C_COMPILER})
            message(STATUS "LEMON C compiler: ${C_COMPILER_LEMON}")
        endif()
        cat("${TD_SUPPORT_DIR}/lemon_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    else()
        add_subdirectory(lemon)
    endif()
endif()

# Force specify CC=cc on MacOS. Because the default CC setting in the generated Makefile has issues finding standard library headers
IF(${TD_DARWIN})
    SET(CONTRIB_CONFIG_ENV "CC=cc")
ENDIF()

# download dependencies
IF(${TD_ASTRA})
    configure_file(${CONTRIB_TMP_FILE} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
    execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download"
        RESULT_VARIABLE result)
    IF(NOT result EQUAL "0")
        message(FATAL_ERROR "CMake step for dowloading dependencies failed: ${result}")
    ENDIF()
ENDIF()

# ================================================================================================
# Build
# ================================================================================================

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
if(${BUILD_WITH_UV})
    if(${TD_LINUX})
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_REL}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL}")

        if("${CMAKE_BUILD_TYPE}" STREQUAL "")
            SET(CMAKE_BUILD_TYPE Release)
        endif()
    endif(${TD_LINUX})
endif(${BUILD_WITH_UV})

if(${BUILD_WITH_S3})
    # INCLUDE_DIRECTORIES($ENV{HOME}/.cos-local.2/include)
    MESSAGE("build with s3: ${BUILD_WITH_S3}")

# cos
elseif(${BUILD_WITH_COS})
    if(${TD_LINUX})
    else()
    endif(${TD_LINUX})
endif()

if(${TD_LINUX} AND ${BUILD_WITH_S3})
  # add_subdirectory(azure-cmake)
endif()

IF(TD_LINUX)
    SET(TZ_OUTPUT_PATH /usr/share/zoneinfo)
ELSEIF(TD_DARWIN)
    SET(TZ_OUTPUT_PATH /var/db/timezone/zoneinfo)
ENDIF()


# ================================================================================================
# Build test
# ================================================================================================
MESSAGE("build with dependency tests: ${BUILD_DEPENDENCY_TESTS}")

if(${BUILD_DEPENDENCY_TESTS})
    add_subdirectory(test EXCLUDE_FROM_ALL)
endif(${BUILD_DEPENDENCY_TESTS})

