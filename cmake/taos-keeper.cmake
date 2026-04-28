# Processing taoskeeper compilation
message(STATUS "Processing taoskeeper compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taoskeeper build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_ENTERPRISE        = ${BUILD_ENTERPRISE}")
message(STATUS "  BUILD_KEEPER            = ${BUILD_KEEPER}")
message(STATUS "  BUILD_ENGINE            = ${BUILD_ENGINE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_KEEPER_DIR           = ${TD_KEEPER_DIR}")
message(STATUS "  TD_BIN_DIR              = ${TD_BIN_DIR}")
message(STATUS "  TD_CFG_DIR              = ${TD_CFG_DIR}")
message(STATUS "")
message(STATUS "Input version variables:")
message(STATUS "  BUILD_VER_NUMBER        = ${BUILD_VER_NUMBER}")
message(STATUS "  BUILD_VER_OSTYPE        = ${BUILD_VER_OSTYPE}")
message(STATUS "  BUILD_VER_CPUTYPE       = ${BUILD_VER_CPUTYPE}")
message(STATUS "  BUILD_VER_DATE          = ${BUILD_VER_DATE}")
message(STATUS "  BUILD_CUS_NAME          = ${BUILD_CUS_NAME}")
message(STATUS "  BUILD_CUS_PROMPT        = ${BUILD_CUS_PROMPT}")
message(STATUS "  BUILD_CUS_PRODUCT_NAME  = ${BUILD_CUS_PRODUCT_NAME}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_EXECUTABLE_SUFFIX = ${CMAKE_EXECUTABLE_SUFFIX}")
message(STATUS "=======================================")
message(STATUS "")

# Verify taoskeeper repo exists
if(NOT EXISTS "${TD_KEEPER_DIR}/go.mod")
  message(FATAL_ERROR "TD_KEEPER_DIR is not a taoskeeper repo: ${TD_KEEPER_DIR}")
endif()

find_program(GO_EXECUTABLE go REQUIRED)
message(STATUS "  GO_EXECUTABLE           = ${GO_EXECUTABLE}")

# Determine keeper mode based on BUILD_ENGINE and BUILD_ENTERPRISE
if(NOT BUILD_ENGINE)
  set(KEEPER_MODE "standalone")
  message(STATUS "Compilation mode: Mode 1 (taoskeeper standalone)")
elseif(BUILD_ENTERPRISE)
  set(KEEPER_MODE "enterprise")
  message(STATUS "Compilation mode: Mode 2 (taoskeeper enterprise)")
else()
  set(KEEPER_MODE "community")
  message(STATUS "Compilation mode: Mode 3 (taoskeeper community)")
endif()

# Get taoskeeper commit ID
execute_process(
  COMMAND git -C "${TD_KEEPER_DIR}" rev-parse HEAD
  OUTPUT_VARIABLE TAOSKEEPER_COMMIT_ID
  RESULT_VARIABLE TAOSKEEPER_COMMIT_RESULT
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT TAOSKEEPER_COMMIT_RESULT EQUAL 0 OR TAOSKEEPER_COMMIT_ID STREQUAL "")
  set(TAOSKEEPER_COMMIT_ID "unknown")
endif()

# Build ldflags
set(_ldflags_list
  "-X 'github.com/taosdata/taoskeeper/version.Version=${BUILD_VER_NUMBER}'"
  "-X 'github.com/taosdata/taoskeeper/version.Gitinfo=${TAOSKEEPER_COMMIT_ID}'"
  "-X 'github.com/taosdata/taoskeeper/version.CommitID=${TAOSKEEPER_COMMIT_ID}'"
  "-X 'github.com/taosdata/taoskeeper/version.BuildInfo=${BUILD_VER_OSTYPE}-${BUILD_VER_CPUTYPE} ${BUILD_VER_DATE}'"
  "-X 'github.com/taosdata/taoskeeper/version.TD_PRODUCT_NAME=${BUILD_CUS_PRODUCT_NAME}'"
  "-X 'github.com/taosdata/taoskeeper/version.CUS_NAME=${BUILD_CUS_NAME}'"
  "-X 'github.com/taosdata/taoskeeper/version.CUS_PROMPT=${BUILD_CUS_PROMPT}'"
)

if(KEEPER_MODE STREQUAL "enterprise")
  list(APPEND _ldflags_list
    "-X 'github.com/taosdata/taoskeeper/version.IsEnterprise=true'"
  )
  set(_config_src "${TD_KEEPER_DIR}/config/taoskeeper_enterprise.toml")
else()
  # Standalone/community share OSS keeper behavior.
  list(APPEND _ldflags_list
    "-X 'github.com/taosdata/taoskeeper/version.IsEnterprise=false'"
  )
  set(_config_src "${TD_KEEPER_DIR}/config/taoskeeper.toml")
endif()

string(JOIN " " _ldflags ${_ldflags_list})

file(GLOB_RECURSE _go_sources CONFIGURE_DEPENDS
  "${TD_KEEPER_DIR}/*.go"
)

set(_binary_output "${TD_BIN_DIR}/taoskeeper${CMAKE_EXECUTABLE_SUFFIX}")
set(_config_output "${TD_CFG_DIR}/taoskeeper.toml")
set(_service_output "${TD_CFG_DIR}/taoskeeper.service")

# Keep module / build / sumdb caches under the build tree so GVM or read-only
# global paths (e.g. ~/.gvm/.../pkg/sumdb) cannot break parallel `make`.
set(_keeper_gomodcache "${CMAKE_BINARY_DIR}/build/taoskeeper/gomodcache")
set(_keeper_gocache "${CMAKE_BINARY_DIR}/build/taoskeeper/gocache")
set(_keeper_gopath "${CMAKE_BINARY_DIR}/build/taoskeeper/gopath")

add_custom_command(
  OUTPUT "${_binary_output}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${TD_BIN_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${TD_CFG_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_keeper_gomodcache}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_keeper_gocache}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_keeper_gopath}"
  COMMAND "${CMAKE_COMMAND}" -E env
          "GOPATH=${_keeper_gopath}"
          "GOMODCACHE=${_keeper_gomodcache}"
          "GOCACHE=${_keeper_gocache}"
          "${GO_EXECUTABLE}" mod download
  COMMAND "${CMAKE_COMMAND}" -E env
          "CGO_ENABLED=0"
          "GO111MODULE=on"
          "GOPATH=${_keeper_gopath}"
          "GOMODCACHE=${_keeper_gomodcache}"
          "GOCACHE=${_keeper_gocache}"
          "${GO_EXECUTABLE}" build
          -o "${_binary_output}"
          -ldflags "${_ldflags}"
          .
  WORKING_DIRECTORY "${TD_KEEPER_DIR}"
  DEPENDS ${_go_sources}
  VERBATIM
  COMMENT "Building taoskeeper (${KEEPER_MODE})"
)

add_custom_command(
  OUTPUT "${_config_output}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${TD_CFG_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
    "${_config_src}"
    "${_config_output}"
  DEPENDS "${_config_src}"
  VERBATIM
  COMMENT "Copying taoskeeper config (${KEEPER_MODE})"
)

add_custom_command(
  OUTPUT "${_service_output}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${TD_CFG_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
    "${TD_KEEPER_DIR}/taoskeeper.service"
    "${_service_output}"
  DEPENDS "${TD_KEEPER_DIR}/taoskeeper.service"
  VERBATIM
  COMMENT "Copying taoskeeper service file"
)

add_custom_target(taoskeeper ALL
  DEPENDS "${_binary_output}" "${_config_output}" "${_service_output}"
)
