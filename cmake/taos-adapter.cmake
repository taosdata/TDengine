# Processing taosadapter compilation
message(STATUS "Processing taosadapter compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taosadapter build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_ENTERPRISE        = ${BUILD_ENTERPRISE}")
message(STATUS "  BUILD_ADAPTER           = ${BUILD_ADAPTER}")
message(STATUS "  BUILD_ENGINE            = ${BUILD_ENGINE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_ENTERPRISE_DIR       = ${TD_ENTERPRISE_DIR}")
message(STATUS "  TD_ADAPTER_DIR          = ${TD_ADAPTER_DIR}")
message(STATUS "  TD_INCLUDE_DIR          = ${TD_INCLUDE_DIR}")
message(STATUS "  TD_LIB_DIR              = ${TD_LIB_DIR}")
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
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "  CMAKE_EXECUTABLE_SUFFIX = ${CMAKE_EXECUTABLE_SUFFIX}")
message(STATUS "==========================================")
message(STATUS "")

# Verify taosadapter repo exists
if(NOT EXISTS "${TD_ADAPTER_DIR}/go.mod")
  message(FATAL_ERROR "TD_ADAPTER_DIR is not a taos-adapter repo: ${TD_ADAPTER_DIR}")
endif()

find_program(GO_EXECUTABLE go REQUIRED)
message(STATUS "  GO_EXECUTABLE           = ${GO_EXECUTABLE}")

# Determine compilation mode based on BUILD_ENGINE and BUILD_ENTERPRISE
if(NOT BUILD_ENGINE)
  set(ADAPTER_MODE "standalone")
  message(STATUS "Compilation mode: Mode 1 (taosadapter standalone, no engine libs)")
elseif(BUILD_ENTERPRISE)
  set(ADAPTER_MODE "enterprise")
  message(STATUS "Compilation mode: Mode 2 (taosadapter with enterprise libs)")
else()
  set(ADAPTER_MODE "community")
  message(STATUS "Compilation mode: Mode 3 (taosadapter with community libs)")
endif()

# Verify engine headers and libs exist for modes 2 and 3
if(ADAPTER_MODE STREQUAL "enterprise" OR ADAPTER_MODE STREQUAL "community")
  if(NOT EXISTS "${TD_INCLUDE_DIR}")
    message(FATAL_ERROR "TD_INCLUDE_DIR does not exist: ${TD_INCLUDE_DIR}")
  endif()
endif()

# Get taosadapter commit ID
execute_process(
  COMMAND git -C "${TD_ADAPTER_DIR}" rev-parse HEAD
  OUTPUT_VARIABLE TAOSADAPTER_COMMIT_ID
  RESULT_VARIABLE TAOSADAPTER_COMMIT_RESULT
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT TAOSADAPTER_COMMIT_RESULT EQUAL 0 OR TAOSADAPTER_COMMIT_ID STREQUAL "")
  set(TAOSADAPTER_COMMIT_ID "unknown")
endif()

# Determine rpath based on platform
if(APPLE)
  set(_rpath "-Wl,-rpath,@loader_path/../lib")
elseif(UNIX)
  set(_rpath "-Wl,-rpath=\$ORIGIN/../lib")
else()
  set(_rpath "")
endif()

# Helper function to configure CGO flags for engine-based compilation (modes 2 and 3)
function(_configure_engine_cgo out_cflags out_ldflags)
  set(_cgo_cflags "-I${TD_INCLUDE_DIR}")
  set(_cgo_ldflags "-L${TD_LIB_DIR}")
  if(NOT _rpath STREQUAL "")
    string(APPEND _cgo_ldflags " ${_rpath}")
  endif()
  # Workaround: on macOS with SDK < 12.0, SecTrustCopyCertificateChain is
  # missing from the SDK but available at runtime.  Tell the linker to allow
  # undefined symbols so that CGO can link successfully.
  if(APPLE)
    execute_process(
      COMMAND xcrun --show-sdk-version
      OUTPUT_VARIABLE _sdk_ver OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
    )
    if(_sdk_ver AND _sdk_ver VERSION_LESS "12.0")
      string(APPEND _cgo_ldflags " -Wl,-undefined,dynamic_lookup")
      message(STATUS "macOS SDK ${_sdk_ver} < 12.0: adding -Wl,-undefined,dynamic_lookup for CGO")
    endif()
  endif()
  set(${out_cflags} "${_cgo_cflags}" PARENT_SCOPE)
  set(${out_ldflags} "${_cgo_ldflags}" PARENT_SCOPE)
endfunction()

# Helper function to build ldflags for version information (shared by modes 2 and 3)
function(_build_engine_ldflags out_var)
  set(_ldflags_list
    "-X 'github.com/taosdata/taosadapter/v3/version.Version=${BUILD_VER_NUMBER}'"
    "-X 'github.com/taosdata/taosadapter/v3/version.CommitID=${TAOSADAPTER_COMMIT_ID}'"
    "-X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${BUILD_VER_OSTYPE}-${BUILD_VER_CPUTYPE} ${BUILD_VER_DATE}'"
    "-X 'github.com/taosdata/taosadapter/v3/version.CUS_NAME=${BUILD_CUS_NAME}'"
    "-X 'github.com/taosdata/taosadapter/v3/version.CUS_PROMPT=${BUILD_CUS_PROMPT}'"
  )
  string(JOIN " " _ldflags ${_ldflags_list})
  set(${out_var} "${_ldflags}" PARENT_SCOPE)
endfunction()

# Configure CGO flags and build parameters based on mode
if(ADAPTER_MODE STREQUAL "standalone")
  # Mode 1: No engine libs, standalone compilation
  set(_common_cgo_cflags "")
  set(_common_cgo_ldflags "")
  set(_go_working_directory "${TD_ADAPTER_DIR}")
  set(_build_label "standalone")
  set(_build_dependencies "")
  
  # Standalone mode: inject CommitID and BuildInfo only
  string(TIMESTAMP _standalone_build_time "%Y-%m-%d %H:%M:%S")
  set(_standalone_build_info "${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR} ${_standalone_build_time}")
  set(_ldflags_list
    "-X 'github.com/taosdata/taosadapter/v3/version.CommitID=${TAOSADAPTER_COMMIT_ID}'"
    "-X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${_standalone_build_info}'"
  )
  string(JOIN " " _ldflags ${_ldflags_list})

elseif(ADAPTER_MODE STREQUAL "enterprise")
  # Mode 2: Enterprise mode with engine libs
  # NOTE: How to distinguish enterprise/community binaries after build:
  #   go version -m <taosadapter>
  #     - enterprise build (this branch) shows `path taosainternal`
  #       because the binary entrypoint is
  #       ${TD_ENTERPRISE_DIR}/source/plugins/taosainternal/main.go
  #     - community build shows `path github.com/taosdata/taosadapter/v3`
  #       because it is built directly from ${TD_ADAPTER_DIR}/main.go
  # This is a handy post-build forensic check when --version output is similar.
  _configure_engine_cgo(_common_cgo_cflags _common_cgo_ldflags)
  set(_go_working_directory "${TD_ENTERPRISE_DIR}/source/plugins/taosainternal")
  set(_build_label "enterprise")
  
  file(GLOB_RECURSE _taosadapter_go_sources CONFIGURE_DEPENDS
    "${TD_ADAPTER_DIR}/*.go"
  )
  file(GLOB_RECURSE _wrapper_go_sources CONFIGURE_DEPENDS
    "${TD_ENTERPRISE_DIR}/source/plugins/taosainternal/*.go"
  )
  set(_build_dependencies ${_wrapper_go_sources} ${_taosadapter_go_sources})
  
  _build_engine_ldflags(_ldflags)

else()
  # Mode 3: Community mode with engine libs
  _configure_engine_cgo(_common_cgo_cflags _common_cgo_ldflags)
  set(_go_working_directory "${TD_ADAPTER_DIR}")
  set(_build_label "community")
  
  file(GLOB_RECURSE _taosadapter_go_sources CONFIGURE_DEPENDS
    "${TD_ADAPTER_DIR}/*.go"
  )
  set(_build_dependencies ${_taosadapter_go_sources})
  
  _build_engine_ldflags(_ldflags)
endif()

set(_binary_output "${TD_BIN_DIR}/taosadapter${CMAKE_EXECUTABLE_SUFFIX}")
set(_config_output "${TD_CFG_DIR}/taosadapter.toml")
set(_service_output "${TD_CFG_DIR}/taosadapter.service")

add_custom_command(
  OUTPUT "${_binary_output}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${TD_BIN_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${TD_CFG_DIR}"
  COMMAND "${GO_EXECUTABLE}" mod download
  COMMAND "${CMAKE_COMMAND}" -E env
          "CGO_ENABLED=1"
          "CGO_CFLAGS=${_common_cgo_cflags}"
          "CGO_LDFLAGS=${_common_cgo_ldflags}"
          "GO111MODULE=on"
          "${GO_EXECUTABLE}" build
          -o "${_binary_output}"
          -ldflags "${_ldflags}"
          .
  WORKING_DIRECTORY "${_go_working_directory}"
  DEPENDS ${_build_dependencies}
  VERBATIM
  COMMENT "Building taosadapter (${_build_label})"
)

add_custom_command(
  OUTPUT "${_config_output}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${TD_CFG_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
    "${TD_ADAPTER_DIR}/example/config/taosadapter.toml"
          "${_config_output}"
  DEPENDS "${TD_ADAPTER_DIR}/example/config/taosadapter.toml"
  VERBATIM
  COMMENT "Copying taosadapter config"
)

add_custom_command(
  OUTPUT "${_service_output}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${TD_CFG_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
    "${TD_ADAPTER_DIR}/taosadapter.service"
          "${_service_output}"
  DEPENDS "${TD_ADAPTER_DIR}/taosadapter.service"
  VERBATIM
  COMMENT "Copying taosadapter service file"
)

add_custom_target(taosadapter ALL
  DEPENDS "${_binary_output}" "${_config_output}" "${_service_output}"
)

# Ensure taosnative is built before linking taosadapter in engine-integrated modes.
if((ADAPTER_MODE STREQUAL "enterprise" OR ADAPTER_MODE STREQUAL "community") AND TARGET taosnative)
  add_dependencies(taosadapter taosnative)
endif()