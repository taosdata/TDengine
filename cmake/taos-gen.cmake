# Processing taos-gen (taosbenchmark) compilation
message(STATUS "")
message(STATUS "Processing taos-gen compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taos-gen build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_GEN               = ${BUILD_GEN}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_GEN_DIR              = ${TD_GEN_DIR}")
message(STATUS "  CMAKE_BINARY_DIR        = ${CMAKE_BINARY_DIR}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "=====================================")
message(STATUS "")

# Verify taos-gen repo exists
if(NOT EXISTS "${TD_GEN_DIR}/CMakeLists.txt")
  message(FATAL_ERROR
    "TD_GEN_DIR is not a taos-gen repo: ${TD_GEN_DIR}")
endif()

if(NOT EXISTS "${TD_GEN_DIR}/conanfile.txt")
  message(FATAL_ERROR
    "TD_GEN_DIR missing conanfile.txt: ${TD_GEN_DIR}")
endif()

# ── Find tools ────────────────────────────────────────────────────────────
find_program(CONAN_EXECUTABLE conan REQUIRED)

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-gen) ───────
set(_gen_base_dir "${CMAKE_BINARY_DIR}/build/taos-gen")
set(_gen_build_dir "${_gen_base_dir}/build")
# Conan output MUST be at ${_gen_build_dir}/conan because taos-gen's
# CMakeLists.txt hard-codes: include(${CMAKE_BINARY_DIR}/conan/conan_toolchain.cmake)
# When cmake -B ${_gen_build_dir}, CMAKE_BINARY_DIR = ${_gen_build_dir},
# so conan files must be at ${_gen_build_dir}/conan/.
set(_gen_conan_dir "${_gen_build_dir}/conan")
set(_gen_stamp "${_gen_base_dir}/taos_gen_built")

message(STATUS "taos-gen build config:")
message(STATUS "  base dir                = ${_gen_base_dir}")
message(STATUS "  cmake build dir         = ${_gen_build_dir}")
message(STATUS "  conan output dir        = ${_gen_conan_dir}")

# ── Map CMAKE_BUILD_TYPE for conan ────────────────────────────────────────
# Conan needs the build_type setting to match
if(CMAKE_BUILD_TYPE)
  set(_gen_conan_build_type "${CMAKE_BUILD_TYPE}")
else()
  set(_gen_conan_build_type "Release")
endif()

# ── Source dependencies for rebuild detection ─────────────────────────────
file(GLOB_RECURSE _gen_sources CONFIGURE_DEPENDS
  "${TD_GEN_DIR}/src/*.cpp"
  "${TD_GEN_DIR}/src/*.h"
  "${TD_GEN_DIR}/inc/*.h"
)

# ── Build steps ───────────────────────────────────────────────────────────
# 1. conan install    → fetch/build C++ dependencies into conan dir
# 2. cmake configure  → configure taos-gen with conan toolchain
# 3. cmake --build    → compile taosbenchmark executable
#
# All intermediate files live under ${_gen_base_dir}.
# source/taos-gen/ is not modified (except CMakeUserPresets.json which
# conan may create — we clean it up).

# On Windows (NMake/JOM), conan_toolchain.cmake contains VS-only
# CMAKE_GENERATOR_PLATFORM & CMAKE_GENERATOR_TOOLSET which must be stripped.
if(WIN32)
  set(_gen_fix_conan_cmd
    COMMAND "${CMAKE_COMMAND}"
            -DINPUT_FILE=${_gen_conan_dir}/conan_toolchain.cmake
            -P "${CMAKE_SOURCE_DIR}/cmake/toolchains/fix-conan-toolchain.cmake"
  )
else()
  set(_gen_fix_conan_cmd)
endif()

add_custom_command(
  OUTPUT "${_gen_stamp}"
  # Create directories
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_gen_base_dir}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_gen_conan_dir}"
  # Step 1: Conan install (fetch dependencies)
  COMMAND "${CONAN_EXECUTABLE}" install "${TD_GEN_DIR}"
          --output-folder "${_gen_conan_dir}"
          --build=missing
          -s "build_type=${_gen_conan_build_type}"
  # Clean CMakeUserPresets.json that conan drops into source tree
  COMMAND "${CMAKE_COMMAND}" -E rm -f "${TD_GEN_DIR}/CMakeUserPresets.json"
  # Step 2: Fix conan toolchain (Windows only) + CMake configure
  ${_gen_fix_conan_cmd}
  COMMAND "${CMAKE_COMMAND}"
          -S "${TD_GEN_DIR}"
          -B "${_gen_build_dir}"
          -G "${CMAKE_GENERATOR}"
          "-DCMAKE_TOOLCHAIN_FILE=${_gen_conan_dir}/conan_toolchain.cmake"
          "-DCMAKE_POLICY_DEFAULT_CMP0091=NEW"
          "-DCMAKE_BUILD_TYPE=${_gen_conan_build_type}"
          "-DCMAKE_C_STANDARD_INCLUDE_DIRECTORIES=${TD_INCLUDE_DIR}"
          "-DCMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES=${TD_INCLUDE_DIR}"
          "-DTSGEN_ENABLE_TEST=${BUILD_TEST}"
          "-DTSGEN_ENABLE_SANITIZER=${BUILD_SANITIZER}"
          "-DTSGEN_ENABLE_COVERAGE=${BUILD_COVERAGE}"
          "-DCMAKE_RUNTIME_OUTPUT_DIRECTORY=${_gen_build_dir}/bin"
  # Step 3: Build
  COMMAND "${CMAKE_COMMAND}" --build "${_gen_build_dir}" -j
  COMMAND "${CMAKE_COMMAND}" -E touch "${_gen_stamp}"
  WORKING_DIRECTORY "${TD_GEN_DIR}"
  DEPENDS "${TD_GEN_DIR}/CMakeLists.txt"
          "${TD_GEN_DIR}/conanfile.txt"
          ${_gen_sources}
  VERBATIM
  COMMENT "Building taos-gen  → ${_gen_build_dir}"
)

add_custom_target(taos_gen ALL
  DEPENDS "${_gen_stamp}"
)
