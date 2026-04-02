# Processing taos-connector-jdbc compilation
message(STATUS "")
message(STATUS "Processing taos-connector-jdbc compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taos-connector-jdbc build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_JDBC              = ${BUILD_JDBC}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_CONNECTOR_JDBC_DIR   = ${TD_CONNECTOR_JDBC_DIR}")
message(STATUS "  CMAKE_BINARY_DIR        = ${CMAKE_BINARY_DIR}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "================================================")
message(STATUS "")

# Verify connector-jdbc repo exists
if(NOT EXISTS "${TD_CONNECTOR_JDBC_DIR}/pom.xml")
  message(FATAL_ERROR
    "TD_CONNECTOR_JDBC_DIR is not a taos-connector-jdbc repo: ${TD_CONNECTOR_JDBC_DIR}")
endif()

find_program(MVN_EXECUTABLE mvn REQUIRED)

# Resolve JAVA_HOME for Maven so plain `make` works too.
set(_jdbc_java_home "$ENV{JAVA_HOME}")
if(NOT "${_jdbc_java_home}" STREQUAL "" AND NOT EXISTS "${_jdbc_java_home}/bin/java")
  # Ignore stale JAVA_HOME from shell and fall back to auto-detection.
  set(_jdbc_java_home "")
endif()
if(CMAKE_SYSTEM_NAME MATCHES "Darwin" AND "${_jdbc_java_home}" STREQUAL "")
  execute_process(
    COMMAND /usr/libexec/java_home
    OUTPUT_VARIABLE _jdbc_java_home
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
  )
endif()

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-connector-jdbc) ─
set(_jdbc_base_dir "${CMAKE_BINARY_DIR}/build/taos-connector-jdbc")
set(_jdbc_output_dir "${_jdbc_base_dir}/output")
set(_jdbc_stamp "${_jdbc_base_dir}/taos_connector_jdbc_built")

# Extract version from pom.xml
file(STRINGS "${TD_CONNECTOR_JDBC_DIR}/pom.xml" _pom_ver_line
  REGEX "<version>[^<]+</version>" LIMIT_COUNT 1)
string(REGEX REPLACE ".*<version>([^<]+)</version>.*" "\\1" _jdbc_version "${_pom_ver_line}")

set(_jdbc_jar_dist    "taos-jdbcdriver-${_jdbc_version}-dist.jar")
set(_jdbc_jar_main    "taos-jdbcdriver-${_jdbc_version}.jar")
set(_jdbc_jar_sources "taos-jdbcdriver-${_jdbc_version}-sources.jar")

# Maven intermediate files stay in source/taos-connector-jdbc/target/
# (Maven cannot reliably redirect this). Only jars are copied to output.
set(_jdbc_target_dir "${TD_CONNECTOR_JDBC_DIR}/target")

message(STATUS "JDBC build config:")
message(STATUS "  base dir                = ${_jdbc_base_dir}")
message(STATUS "  output dir              = ${_jdbc_output_dir}")
message(STATUS "  intermediate dir        = ${_jdbc_target_dir} (Maven default)")
message(STATUS "  JDBC version            = ${_jdbc_version}")
if(NOT "${_jdbc_java_home}" STREQUAL "")
  message(STATUS "  JAVA_HOME               = ${_jdbc_java_home}")
endif()
message(STATUS "  artifacts:")
message(STATUS "    dist jar              = ${_jdbc_jar_dist}")
message(STATUS "    main jar              = ${_jdbc_jar_main}")
message(STATUS "    sources jar           = ${_jdbc_jar_sources}")

file(GLOB_RECURSE _jdbc_sources CONFIGURE_DEPENDS
  "${TD_CONNECTOR_JDBC_DIR}/src/*.java"
)

set(_jdbc_mvn_command
  "${MVN_EXECUTABLE}"
  -f "${TD_CONNECTOR_JDBC_DIR}/pom.xml"
  -DskipTests
  -Djacoco.skip=true
  package
)
if(NOT "${_jdbc_java_home}" STREQUAL "")
  set(_jdbc_mvn_command
    "${CMAKE_COMMAND}" -E env
    "JAVA_HOME=${_jdbc_java_home}"
    "${MVN_EXECUTABLE}"
    -f "${TD_CONNECTOR_JDBC_DIR}/pom.xml"
    -DskipTests
    -Djacoco.skip=true
    package
  )
endif()

# Maven intermediate files stay in source/target/ (cannot be redirected).
# We only copy the 3 jar artifacts to the output directory.
add_custom_command(
  OUTPUT "${_jdbc_stamp}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_jdbc_output_dir}"
  # Maven build (incremental — target/ persists in source)
  COMMAND ${_jdbc_mvn_command}
  # Copy 3 jars to output dir
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_jdbc_target_dir}/${_jdbc_jar_dist}"
          "${_jdbc_output_dir}/${_jdbc_jar_dist}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_jdbc_target_dir}/${_jdbc_jar_main}"
          "${_jdbc_output_dir}/${_jdbc_jar_main}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_jdbc_target_dir}/${_jdbc_jar_sources}"
          "${_jdbc_output_dir}/${_jdbc_jar_sources}"
  COMMAND "${CMAKE_COMMAND}" -E touch "${_jdbc_stamp}"
  WORKING_DIRECTORY "${TD_CONNECTOR_JDBC_DIR}"
  DEPENDS "${TD_CONNECTOR_JDBC_DIR}/pom.xml"
          ${_jdbc_sources}
  VERBATIM
  COMMENT "Building taos-connector-jdbc → ${_jdbc_output_dir}"
)

add_custom_target(taos_connector_jdbc ALL
  DEPENDS "${_jdbc_stamp}"
)
