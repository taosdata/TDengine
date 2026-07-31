if(NOT ARROW_SOURCE_DIR)
    message(FATAL_ERROR "ARROW_SOURCE_DIR is required")
endif()

set(_toolchain_file "${ARROW_SOURCE_DIR}/cpp/cmake_modules/ThirdpartyToolchain.cmake")
if(NOT EXISTS "${_toolchain_file}")
    message(FATAL_ERROR "Arrow ThirdpartyToolchain.cmake not found: ${_toolchain_file}")
endif()

file(READ "${_toolchain_file}" _content)

if(_content MATCHES "TDENGINE_GCC13_ABSL_STDINT_FIX")
    message(STATUS "Arrow ThirdpartyToolchain.cmake already patched for GCC 13+ Abseil stdint fix")
    return()
endif()

set(_old_condition "if(CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 13.0)")
set(_new_condition [=[
# TDENGINE_GCC13_ABSL_STDINT_FIX_BEGIN
if((CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_COMPILER_IS_GNUCC) AND
   CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 13.0)
# TDENGINE_GCC13_ABSL_STDINT_FIX_END
]=])

if(NOT _content MATCHES "CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 13\\.0")
    message(FATAL_ERROR
        "Failed to locate Arrow Abseil GCC 13+ compatibility condition in ${_toolchain_file}")
endif()

string(REPLACE "${_old_condition}" "${_new_condition}" _content "${_content}")

file(WRITE "${_toolchain_file}" "${_content}")
message(STATUS "Patched Arrow ThirdpartyToolchain.cmake for GCC 13+ Abseil stdint compatibility")
