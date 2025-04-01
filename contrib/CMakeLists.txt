# ================================================================================================
# Download
# ================================================================================================
function(cat IN_FILE OUT_FILE)
    file(READ ${IN_FILE} CONTENTS)
    file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

if(${BUILD_WITH_S3})
    file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.2/)

elseif(${BUILD_WITH_COS})
    message(FATAL_ERROR "freemine: not implemented yet")
    # set(CONTRIB_TMP_FILE3 "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in3")
    # configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})

    # if(${BUILD_WITH_COS})
    #     file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.1/)
    #     cat("${TD_SUPPORT_DIR}/mxml_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})
    #     cat("${TD_SUPPORT_DIR}/apr_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})
    # endif(${BUILD_WITH_COS})

    # configure_file(${CONTRIB_TMP_FILE3} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
    # execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
    #     WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
    # execute_process(COMMAND "${CMAKE_COMMAND}" --build .
    #     WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")

    # set(CONTRIB_TMP_FILE2 "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in2")
    # configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE2})

    # if(${BUILD_WITH_COS})
    #     cat("${TD_SUPPORT_DIR}/apr-util_CMakeLists.txt.in" ${CONTRIB_TMP_FILE2})
    # endif(${BUILD_WITH_COS})

    # configure_file(${CONTRIB_TMP_FILE2} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
    # execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
    #     WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
    # execute_process(COMMAND "${CMAKE_COMMAND}" --build .
    #     WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
endif()

set(CONTRIB_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# cat("${TD_SUPPORT_DIR}/zstd_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# s3
if(${BUILD_WITH_S3})
    message(FATAL_ERROR "freemine: not implemented yet")
    cat("${TD_SUPPORT_DIR}/xml2_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/libs3_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/azure_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_S3)

# cos
elseif(${BUILD_WITH_COS})
    message(FATAL_ERROR "freemine: not implemented yet")
    # cat("${TD_SUPPORT_DIR}/mxml_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    # cat("${TD_SUPPORT_DIR}/apr_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    # cat("${TD_SUPPORT_DIR}/apr-util_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/cos_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
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
# configure_file(${CONTRIB_TMP_FILE} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
# execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
#     WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download"
#     RESULT_VARIABLE result)
# IF(NOT result EQUAL "0")
#     message(FATAL_ERROR "CMake step for dowloading dependencies failed: ${result}")
# ENDIF()
# 
# execute_process(COMMAND "${CMAKE_COMMAND}" --build .
#     WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download"
#     RESULT_VARIABLE result)
# IF(NOT result EQUAL "0")
#     message(FATAL_ERROR "CMake step for building dependencies failed: ${result}")
# ENDIF()

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
    INCLUDE_DIRECTORIES($ENV{HOME}/.cos-local.2/include)
    MESSAGE("build with s3: ${BUILD_WITH_S3}")

# cos
elseif(${BUILD_WITH_COS})
    if(${TD_LINUX})
        set(CMAKE_PREFIX_PATH $ENV{HOME}/.cos-local.1)

        # ADD_DEFINITIONS(-DMINIXML_LIBRARY=${CMAKE_BINARY_DIR}/build/lib/libxml.a)
        option(ENABLE_TEST "Enable the tests" OFF)
        INCLUDE_DIRECTORIES($ENV{HOME}/.cos-local.1/include)

        # MESSAGE("$ENV{HOME}/.cos-local.1/include")
        set(CMAKE_BUILD_TYPE Release)
        set(ORIG_CMAKE_PROJECT_NAME ${CMAKE_PROJECT_NAME})
        set(CMAKE_PROJECT_NAME cos_c_sdk)

        add_subdirectory(cos-c-sdk-v5 EXCLUDE_FROM_ALL)
        target_include_directories(
            cos_c_sdk
            PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cos-c-sdk-v5/cos_c_sdk>
        )

        set(CMAKE_PROJECT_NAME ${ORIG_CMAKE_PROJECT_NAME})
    else()
    endif(${TD_LINUX})
endif()

if(${TD_LINUX} AND ${BUILD_WITH_S3})
  set(ORIG_CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
  string(REPLACE " -Werror " " " CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
  add_subdirectory(xml2-cmake)
  set(CMAKE_C_FLAGS ${ORIG_CMAKE_C_FLAGS})

  add_subdirectory(azure-cmake)
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
