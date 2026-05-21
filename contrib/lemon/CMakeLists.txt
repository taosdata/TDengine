add_executable(lemon lemon.c)
# lemon is a host-side parser generator tool; it must not be built with
# sanitizer instrumentation because devtoolset-7 only ships 32-bit ubsan libs
# and the devtoolset-10 workaround in LIBRARY_PATH does not cover host tools.
if(BUILD_SANITIZER AND NOT TD_WINDOWS)
  target_compile_options(lemon PRIVATE
    -fno-sanitize=address
    -fno-sanitize=undefined
    -fno-sanitize=float-divide-by-zero
    -fno-sanitize=float-cast-overflow
  )
  target_link_options(lemon PRIVATE
    -fno-sanitize=address
    -fno-sanitize=undefined
    -fno-sanitize=float-divide-by-zero
    -fno-sanitize=float-cast-overflow
  )
elseif(BUILD_SANITIZER AND TD_WINDOWS)
  # MSVC has no per-target equivalent of GCC's -fno-sanitize=address.
  # Strip /fsanitize=address and its companion macros from CMAKE_C/CXX_FLAGS
  # in this directory scope only (does NOT affect parent or sibling targets).
  # Without this, lemon.exe would require clang_rt.asan_dynamic*.dll at build
  # time when the build system invokes it to generate taos_lemon_sql.c, and
  # that DLL is not guaranteed to be in PATH during the cmake build step.
  foreach(_v CMAKE_C_FLAGS CMAKE_CXX_FLAGS
             CMAKE_C_FLAGS_RELEASE CMAKE_C_FLAGS_DEBUG
             CMAKE_CXX_FLAGS_RELEASE CMAKE_CXX_FLAGS_DEBUG)
    string(REPLACE "/fsanitize=address" "" ${_v} "${${_v}}")
    string(REPLACE "/D_DISABLE_VECTOR_ANNOTATION=1" "" ${_v} "${${_v}}")
    string(REPLACE "/D_DISABLE_STRING_ANNOTATION=1" "" ${_v} "${${_v}}")
  endforeach()
endif()

