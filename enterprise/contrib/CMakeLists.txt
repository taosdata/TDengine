# ================================================================================================
# Download
# ================================================================================================
function(cat IN_FILE OUT_FILE)
  file(READ ${IN_FILE} CONTENTS)
  file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

set(CONTRIB_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
configure_file("${TD_ENTERPRISE_DIR}/cmake/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# community
IF(NOT EXISTS ${TD_COMMUNITY_DIR}/source)
  EXECUTE_PROCESS(
    COMMAND git rev-parse --abbrev-ref HEAD
    OUTPUT_VARIABLE TD_VERSION
  )
  cat("${TD_ENTERPRISE_DIR}/cmake/community_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
ENDIF()

IF(NOT EXISTS ${TD_ENTERPRISE_DIR}/src/plugins/taosx)
  cat("${TD_ENTERPRISE_DIR}/cmake/taosx_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
ENDIF()

# grant-lib
# IF (NOT BUILD_WITH_CFG_GRANTS)
cat("${TD_ENTERPRISE_DIR}/cmake/grant-lib_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
# ENDIF ()

# download dependencies
configure_file(${CONTRIB_TMP_FILE} "${TD_ENTERPRISE_DIR}/contrib/deps-download/CMakeLists.txt")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
  WORKING_DIRECTORY "${TD_ENTERPRISE_DIR}/contrib/deps-download"
)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build .
  WORKING_DIRECTORY "${TD_ENTERPRISE_DIR}/contrib/deps-download"
)
