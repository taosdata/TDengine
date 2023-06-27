# ================================================================================================
# Download
# ================================================================================================
function(cat IN_FILE OUT_FILE)
    file(READ ${IN_FILE} CONTENTS)
    file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

# cloud
if(${BUILD_CLOUD})
        SET(BUILD_WITH_CFG_GRANTS ON CACHE BOOL "cloud build cfg grants" FORCE)
        SET(GRANT_CFG_INCLUDE_DIR "${TD_SOURCE_DIR}/enterprise/src/plugins/grant/inc" CACHE PATH "cfg grants path" FORCE)
endif(${BUILD_CLOUD})

set(CONTRIB_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
configure_file("${TD_SOURCE_DIR}/enterprise/cmake/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# community
IF (NOT EXISTS ${TD_SOURCE_DIR}/community/source)
        EXECUTE_PROCESS(
        COMMAND  git rev-parse --abbrev-ref HEAD
        OUTPUT_VARIABLE TD_VERSION
        )
        cat("${TD_SOURCE_DIR}/enterprise/cmake/community_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
ENDIF ()

# grant-lib
# IF (NOT BUILD_WITH_CFG_GRANTS)
        cat("${TD_SOURCE_DIR}/enterprise/cmake/grant-lib_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
# ENDIF ()

#IF (NOT EXISTS ${TD_SOURCE_DIR}/enterprise/src/plugins/taosx)
#        cat("${TD_SOURCE_DIR}/enterprise/cmake/taosx_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
#ENDIF ()

#IF (NOT EXISTS ${TD_SOURCE_DIR}/enterprise/src/plugins/explorer)
#        cat("${TD_SOURCE_DIR}/enterprise/cmake/explorer_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
#ENDIF ()

# download dependencies
configure_file(${CONTRIB_TMP_FILE} "${TD_SOURCE_DIR}/enterprise/contrib/deps-download/CMakeLists.txt")
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${TD_SOURCE_DIR}/enterprise/contrib/deps-download")
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${TD_SOURCE_DIR}/enterprise/contrib/deps-download")
