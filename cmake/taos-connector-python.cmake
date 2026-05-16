# Processing taos-connector-python compilation
message(STATUS "")
message(STATUS "Processing taos-connector-python compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taos-connector-python build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_PYTHON            = ${BUILD_PYTHON}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_CONNECTOR_PYTHON_DIR = ${TD_CONNECTOR_PYTHON_DIR}")
message(STATUS "  CMAKE_BINARY_DIR        = ${CMAKE_BINARY_DIR}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "==================================================")
message(STATUS "")

# Verify connector-python repo exists
if(NOT EXISTS "${TD_CONNECTOR_PYTHON_DIR}/pyproject.toml")
  message(FATAL_ERROR
    "TD_CONNECTOR_PYTHON_DIR is not a taos-connector-python repo: ${TD_CONNECTOR_PYTHON_DIR}")
endif()

find_program(PYTHON_EXECUTABLE python3 REQUIRED)

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-connector-python) ─
set(_python_base_dir "${CMAKE_BINARY_DIR}/build/taos-connector-python")
set(_python_output_dir "${_python_base_dir}/output")

# ── taospy: pure-Python wheel via PEP 517 build ──────────────────────────
set(_taospy_stamp "${_python_base_dir}/taospy_built")

file(GLOB_RECURSE _taospy_sources CONFIGURE_DEPENDS
  "${TD_CONNECTOR_PYTHON_DIR}/taos/*.py"
  "${TD_CONNECTOR_PYTHON_DIR}/taosrest/*.py"
)

message(STATUS "Python build config:")
message(STATUS "  base dir                = ${_python_base_dir}")
message(STATUS "  output dir              = ${_python_output_dir}")

add_custom_command(
  OUTPUT "${_taospy_stamp}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_python_output_dir}"
  COMMAND "${PYTHON_EXECUTABLE}" -m build
          --outdir "${_python_output_dir}"
          "${TD_CONNECTOR_PYTHON_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E touch "${_taospy_stamp}"
  WORKING_DIRECTORY "${TD_CONNECTOR_PYTHON_DIR}"
  DEPENDS "${TD_CONNECTOR_PYTHON_DIR}/pyproject.toml"
          ${_taospy_sources}
  VERBATIM
  COMMENT "Building taospy wheel + sdist → ${_python_output_dir}"
)

# ── taos-ws-py: Rust native extension via maturin ─────────────────────────
set(_taos_ws_py_dir "${TD_CONNECTOR_PYTHON_DIR}/taos-ws-py")
set(_taos_ws_py_stamp "${_python_base_dir}/taos_ws_py_built")
set(_taos_ws_py_target_dir "${_python_base_dir}/target")

if(EXISTS "${_taos_ws_py_dir}/Cargo.toml")
  # Derive the Python scripts dir so we can find maturin installed via pip
  execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" -c "import sysconfig; print(sysconfig.get_path('scripts'))"
    OUTPUT_VARIABLE _python_scripts_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE _scripts_result
  )
  if(_scripts_result EQUAL 0 AND _python_scripts_dir)
    find_program(MATURIN_EXECUTABLE maturin HINTS "${_python_scripts_dir}" REQUIRED)
  else()
    find_program(MATURIN_EXECUTABLE maturin REQUIRED)
  endif()

  file(GLOB_RECURSE _taos_ws_py_sources CONFIGURE_DEPENDS
    "${_taos_ws_py_dir}/src/*.rs"
  )

  # Map CMAKE_BUILD_TYPE to maturin profile
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(_maturin_profile_args "")
    set(_maturin_profile_label "debug")
  else()
    set(_maturin_profile_args --release)
    set(_maturin_profile_label "release")
  endif()

  message(STATUS "taos-ws-py build config:")
  message(STATUS "  maturin profile         = ${_maturin_profile_label}")
  message(STATUS "  CARGO_TARGET_DIR        = ${_taos_ws_py_target_dir}")

  add_custom_command(
    OUTPUT "${_taos_ws_py_stamp}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${_python_output_dir}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taos_ws_py_target_dir}"
    COMMAND "${CMAKE_COMMAND}" -E env
            "CARGO_TARGET_DIR=${_taos_ws_py_target_dir}"
            "${MATURIN_EXECUTABLE}" build
            --out "${_python_output_dir}"
            --target-dir "${_taos_ws_py_target_dir}"
            ${_maturin_profile_args}
    COMMAND "${CMAKE_COMMAND}" -E touch "${_taos_ws_py_stamp}"
    WORKING_DIRECTORY "${_taos_ws_py_dir}"
    DEPENDS "${_taos_ws_py_dir}/Cargo.toml"
            "${_taos_ws_py_dir}/Cargo.lock"
            ${_taos_ws_py_sources}
    VERBATIM
    COMMENT "Building taos-ws-py (${_maturin_profile_label}) → ${_python_output_dir}"
  )

  add_custom_target(taos_connector_python ALL
    DEPENDS "${_taospy_stamp}" "${_taos_ws_py_stamp}"
  )
else()
  message(STATUS "taos-ws-py subdir not found, building taospy only")
  add_custom_target(taos_connector_python ALL
    DEPENDS "${_taospy_stamp}"
  )
endif()
