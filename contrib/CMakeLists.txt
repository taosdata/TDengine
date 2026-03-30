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
add_subdirectory(lemon)

# Force specify CC=cc on MacOS. Because the default CC setting in the generated Makefile has issues finding standard library headers
IF(${TD_DARWIN})
    SET(CONTRIB_CONFIG_ENV "CC=cc")
ENDIF()

# # download dependencies

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

