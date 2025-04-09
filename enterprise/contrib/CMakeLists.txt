# ================================================================================================
# Download
# ================================================================================================
function(cat IN_FILE OUT_FILE)
  file(READ ${IN_FILE} CONTENTS)
  file(APPEND ${OUT_FILE} ${CONTENTS})
endfunction(cat IN_FILE OUT_FILE)

set(CONTRIB_TMP_FILE ${CMAKE_BINARY_DIR}/in/deps_tmp_CMakeLists.txt.in)
configure_file(${TD_ENTERPRISE_DIR}/cmake/in/deps_CMakeLists.txt.in ${CONTRIB_TMP_FILE})

IF(NOT EXISTS ${TD_COMMUNITY_DIR}/source)
  EXECUTE_PROCESS(
    COMMAND git rev-parse --abbrev-ref HEAD
    OUTPUT_VARIABLE TD_VERSION
  )
  cat(${TD_ENTERPRISE_DIR}/cmake/in/community_CMakeLists.txt.in ${CONTRIB_TMP_FILE})
ENDIF()

IF(NOT EXISTS ${TD_ENTERPRISE_DIR}/src/plugins/taosx)
  cat(${TD_ENTERPRISE_DIR}/cmake/in/taosx_CMakeLists.txt.in ${CONTRIB_TMP_FILE})
ENDIF()

cat(${TD_ENTERPRISE_DIR}/cmake/in/grant-lib_CMakeLists.txt.in ${CONTRIB_TMP_FILE})

# download dependencies
configure_file(${CONTRIB_TMP_FILE} ${TD_ENTERPRISE_DIR}/contrib/deps-download/CMakeLists.txt)
execute_process(
  COMMAND ${CMAKE_COMMAND} -G ${CMAKE_GENERATOR} .
  WORKING_DIRECTORY ${TD_ENTERPRISE_DIR}/contrib/deps-download
)
execute_process(
  COMMAND ${CMAKE_COMMAND} --build .
  WORKING_DIRECTORY ${TD_ENTERPRISE_DIR}/contrib/deps-download
)
