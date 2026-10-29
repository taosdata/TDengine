# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set(CMAKE_VERBOSE_MAKEFILE FALSE)

# set output directory
SET(TD_BUILD_DIR ${CMAKE_BINARY_DIR}/build)
SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TD_BUILD_DIR}/bin)
SET(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${TD_BUILD_DIR}/lib)
if(TD_WINDOWS)
    # adapt to the rule of DLL searching on Windows
    SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${TD_BUILD_DIR}/bin)
else()
    # better set rpath to $ORIGIN/../lib accordingly
    SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${TD_BUILD_DIR}/lib)
endif()
SET(TD_TESTS_OUTPUT_DIR ${PROJECT_BINARY_DIR}/test)

MESSAGE(STATUS "Project source directory: " ${PROJECT_SOURCE_DIR})
MESSAGE(STATUS "Project binary files output path: " ${PROJECT_BINARY_DIR})
MESSAGE(STATUS "Project executable files output path: " ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
MESSAGE(STATUS "Project library files output path: " ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY})

if(TD_DARWIN_64 AND BUILD_TEST)
  add_definitions(-DCOMPILER_SUPPORTS_CXX13)
endif()

add_definitions(
  -DUSE_AUDIT
  -DUSE_GEOS
  -DUSE_UDF
  -DUSE_STREAM
  -DUSE_PCRE2
  -DUSE_RSMA
  -DUSE_TSMA
  -DUSE_TQ
  -DUSE_TOPIC
  -DUSE_MONITOR
  -DUSE_REPORT
)

if(BUILD_ASTRA)
  add_definitions(-DTD_ASTRA)
endif()

if(BUILD_ASTRA_RPC)
  add_definitions(-DTD_ASTRA_RPC)
endif()

if(BUILD_TAOSD_INTEGRATED)
  add_definitions(-DTAOSD_INTEGRATED)
endif()

if(BUILD_AS_LIB)
  add_definitions(-DTD_AS_LIB)
endif()

# build.sh and the coverage container pass -DWEBSOCKET=true. The option
# that actually selects the driver is BUILD_WEBSOCKET.
if(WEBSOCKET AND NOT BUILD_WEBSOCKET)
  set(BUILD_WEBSOCKET ON CACHE BOOL "Enable websocket" FORCE)
endif()

if(BUILD_WEBSOCKET)
  set(TD_WEBSOCKET TRUE)
  message(STATUS "Enable websocket")
  add_definitions(-DWEBSOCKET)
else()
  set(TD_WEBSOCKET FALSE)
endif()

if(BUILD_TOOLS)
  message(STATUS "Will build taos_tools!")
  set(TD_TAOS_TOOLS TRUE)
else()
  message(STATUS "Will _not_ build taos_tools!")
  set(TD_TAOS_TOOLS FALSE)
endif()

IF(BUILD_FLEX_DEPLOY)
    ADD_DEFINITIONS(-DTD_FLEX_DEPLOY)
ENDIF()

IF(${BUILD_SHARED_STORAGE})
  add_definitions(-DUSE_SHARED_STORAGE)
  IF(${BUILD_WITH_S3})
    add_definitions(-DUSE_S3)
  ENDIF ()
ENDIF ()

if(BUILD_WITH_COS)
    message(FATAL_ERROR "freemine: not implemented yet")
endif()

# Enable advanced security features
IF(BUILD_ADVANCED_SECURITY)
    ADD_DEFINITIONS(-DTD_ENABLE_ADVANCED_SECURITY)
ENDIF()

IF(BUILD_ASSERT_NOT_CORE)
    ADD_DEFINITIONS(-DASSERT_NOT_CORE)
    MESSAGE(STATUS "Disable assert not core")
ELSE()
    MESSAGE(STATUS "Enable assert not core")
ENDIF()

SET(TAOS_LIB taos)
SET(TAOS_NATIVE_LIB taosnative)
SET(TAOS_NATIVE_LIB_STATIC taosnative_static)

function(taos_configure_macos_client_rpath target_name)
  if(NOT TD_DARWIN)
    return()
  endif()

  set_target_properties(${target_name} PROPERTIES
    MACOSX_RPATH TRUE
    BUILD_WITH_INSTALL_RPATH TRUE
    SKIP_BUILD_RPATH FALSE
    BUILD_RPATH "@loader_path;@loader_path/../driver;/usr/local/lib"
    INSTALL_RPATH "@loader_path;@loader_path/../driver;/usr/local/lib"
  )
endfunction()

if(BUILD_TSZ_ENABLED)
  message(STATUS "build with TSZ enabled")
  add_definitions(-DTD_TSZ)
  set(VAR_TSZ "TSZ" CACHE INTERNAL "global variant tsz")
else()
  set(VAR_TSZ "" CACHE INTERNAL "global variant empty")
endif()

IF(TD_WINDOWS)
    MESSAGE("${Yellow} set compiler flag for Windows! ${ColourReset}")

    IF(CMAKE_BUILD_TYPE MATCHES "Release")
        MESSAGE("${Green} will build Release version! ${ColourReset}")
        # NOTE: let cmake to choose default compile options
        message(STATUS "do NOT forget to remove the following line and check if it works or not!!!")
        # /Zi  : generate a separate PDB file (previously /Zi- which disabled it entirely).
        # The PDB is NOT shipped to the user but must be archived internally per version
        # so that crash dumps from the field can be symbolicated with WinDbg / VS.
        IF(${BUILD_SANITIZER})
            MESSAGE("${Green} will build with AddressSanitizer (MSVC ASan)! ${ColourReset}")
            # /fsanitize=address: MSVC ASan (incompatible with /GL and /O2)
            # _DISABLE_VECTOR_ANNOTATION/_DISABLE_STRING_ANNOTATION: suppress STL ASan
            # annotations to avoid LNK2038 mismatch with pre-built libs (e.g. rocksdb)
            # that were compiled without /fsanitize=address.
            SET(COMMON_FLAGS "/W3 /D_WIN32 /DWIN32 /Zi /FS /O1 /MD /fsanitize=address /D_DISABLE_VECTOR_ANNOTATION=1 /D_DISABLE_STRING_ANNOTATION=1")
        ELSE()
            SET(COMMON_FLAGS "/W3 /D_WIN32 /DWIN32 /Zi /FS /O2 /GL /MD")
        ENDIF()
    ELSE()
        MESSAGE("${Green} will build Debug version! ${ColourReset}")
        # NOTE: let cmake to choose default compile options
        IF(${BUILD_SANITIZER})
            MESSAGE("${Green} will build Debug with AddressSanitizer (MSVC ASan)! ${ColourReset}")
            # /fsanitize=address is compatible with /MDd; no /GL or /RTC1 in Debug
            # so there are no incompatibility constraints unlike Release.
            SET(COMMON_FLAGS "/w /D_WIN32 /DWIN32 /Zi /FS /MDd /fsanitize=address /D_DISABLE_VECTOR_ANNOTATION=1 /D_DISABLE_STRING_ANNOTATION=1")
        ELSE()
            SET(COMMON_FLAGS "/w /D_WIN32 /DWIN32 /Zi /FS /MDd")
        ENDIF()
    ENDIF()

    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /MANIFEST:NO /FORCE:MULTIPLE")
    # /DEBUG:FULL  keep full debug info in the PDB for Release builds so that
    # crash dumps collected from the field can be fully symbolicated.
    # The flag is harmless for Debug builds (they already carry full info).
    SET(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} /DEBUG:FULL /OPT:REF /OPT:ICF")
    SET(CMAKE_SHARED_LINKER_FLAGS_RELEASE "${CMAKE_SHARED_LINKER_FLAGS_RELEASE} /DEBUG:FULL /OPT:REF /OPT:ICF")

    # IF (MSVC AND (MSVC_VERSION GREATER_EQUAL 1900))
    # SET(COMMON_FLAGS "${COMMON_FLAGS} /Wv:18")
    # ENDIF ()
    IF(CMAKE_DEPFILE_FLAGS_C)
        SET(CMAKE_DEPFILE_FLAGS_C "")
    ENDIF()

    IF(CMAKE_DEPFILE_FLAGS_CXX)
        SET(CMAKE_DEPFILE_FLAGS_CXX "")
    ENDIF()

    IF(CMAKE_C_FLAGS_DEBUG)
        SET(CMAKE_C_FLAGS_DEBUG "" CACHE STRING "" FORCE)
    ENDIF()

    IF(CMAKE_CXX_FLAGS_DEBUG)
        SET(CMAKE_CXX_FLAGS_DEBUG "" CACHE STRING "" FORCE)
    ENDIF()

    # ref: https://learn.microsoft.com/en-us/cpp/error-messages/compiler-warnings/compiler-warning-level-1-c4819?view=msvc-170
    set(_c_cxx_flags_list
        /WX
        /wd4311      # 'variable' : pointer truncation from 'type' to 'type'
        /wd4312      # 'operation' : conversion from 'type1' to 'type2' of greater size
        /wd4022      # 'function' : pointer mismatch for actual parameter 'number'
        /wd4013      # 'function' undefined; assuming extern returning int
        /wd4090      # 'operation' : different 'modifier' qualifiers
        /wd4996      # Your code uses a function, class member, variable, or typedef that's marked deprecated
        /wd4819      # The file contains a character that cannot be represented in the current code page (number)
        /wd4101      # The local variable is never used
        /wd4244      # 'argument' : conversion from 'type1' to 'type2', possible loss of data
        /wd4267      # 'var' : conversion from 'size_t' to 'type', possible loss of data
        /wd4098      # 'function' : void function returning a value
        /wd4047      # 'operator' : 'identifier1' differs in levels of indirection from 'identifier2'
        /wd4133      # 'expression': incompatible types - from 'type1' to 'type2'
        /wd4715      # 'function' : not all control paths return a value
        /wd4018      # 'token' : signed/unsigned mismatch
        /wd4716      # 'function' must return a value
        /wd4305      # 'conversion': truncation from 'type1' to 'type2'
        /wd4102      # 'label' : unreferenced label
        /wd4146      # unary minus operator applied to unsigned type, result still unsigned
        /wd4005      # 'identifier' : macro redefinition
        /wd4273      # 'function' : inconsistent DLL linkage
        /wd4068      # unknown pragma
        /wd4003      # not enough arguments for function-like macro invocation 'identifier'
        /wd4081      # expected 'token1'; found 'token2'
        /wd4113      # 'identifier1' differs in parameter lists from 'identifier2'
        /wd4477      # 'function' : format string 'string' requires an argument of type 'type', but variadic argument number has type 'type'
        /wd4293      # 'operator' : shift count negative or too big, undefined behavior
        /wd4805      # 'operation' : unsafe mix of type 'type' and type 'type' in operation
        /wd4334      # 'operator': result of 32-bit shift implicitly converted to 64 bits (was 64-bit shift intended?)
        /wd4307      # 'operator' : signed integral constant overflow
        /wd4200      # nonstandard extension used: zero-sized array in struct/union
                     # C++ only: This member will be ignored by a defaulted constructor or copy/move assignment operator
        /wd4309      # 'conversion' : truncation of constant value
        /wd4028      # formal parameter 'number' different from declaration
    )
    string(JOIN " " _c_cxx_flags ${_c_cxx_flags_list})
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS} ${_c_cxx_flags}")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS} ${_c_cxx_flags}")

ELSE()
    IF(TD_DARWIN)
        set(CMAKE_MACOSX_RPATH 0)
    ENDIF()

    set(_c_cxx_flags_list
      -Wno-unused-result
    )
    string(JOIN " " _c_cxx_flags ${_c_cxx_flags_list})
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS} ${_c_cxx_flags}")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS} ${_c_cxx_flags}")

    IF(BUILD_COVERAGE)
        MESSAGE(STATUS "Test coverage mode, add extra flags")
        SET(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
        SET(GCC_COVERAGE_LINK_FLAGS "-lgcov --coverage")
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")
    ENDIF()

    # disable all assert
    IF(DISABLE_ASSERT OR DISABLE_ASSERTS)
        ADD_DEFINITIONS(-DDISABLE_ASSERT)
        MESSAGE(STATUS "Disable all asserts")
    ENDIF()

    INCLUDE(CheckCCompilerFlag)
    INCLUDE(CheckCXXCompilerFlag)

    IF(TD_ARM_64 OR TD_ARM_32 OR TD_DARWIN_ARM64)
        SET(COMPILER_SUPPORT_SSE42 false)
    ELSEIF(("${CMAKE_C_COMPILER_ID}" MATCHES "Clang") OR ("${CMAKE_C_COMPILER_ID}" MATCHES "AppleClang"))
        SET(COMPILER_SUPPORT_SSE42 true)
        MESSAGE(STATUS "Always enable sse4.2 for Clang/AppleClang")
    ELSE()
        CHECK_C_COMPILER_FLAG("-msse4.2" COMPILER_SUPPORT_SSE42)
    ENDIF()

    IF(TD_ARM_64 OR TD_ARM_32 OR TD_DARWIN_ARM64)
        SET(COMPILER_SUPPORT_FMA false)
        SET(COMPILER_SUPPORT_AVX false)
        SET(COMPILER_SUPPORT_AVX2 false)
        SET(COMPILER_SUPPORT_AVX512F false)
        SET(COMPILER_SUPPORT_AVX512BMI false)
        SET(COMPILER_SUPPORT_AVX512VL false)
    ELSE()
        CHECK_C_COMPILER_FLAG("-mfma" COMPILER_SUPPORT_FMA)
        CHECK_C_COMPILER_FLAG("-mavx" COMPILER_SUPPORT_AVX)
        CHECK_C_COMPILER_FLAG("-mavx2" COMPILER_SUPPORT_AVX2)
        CHECK_C_COMPILER_FLAG("-mavx512f" COMPILER_SUPPORT_AVX512F)
        CHECK_C_COMPILER_FLAG("-mavx512vbmi" COMPILER_SUPPORT_AVX512BMI)
        CHECK_C_COMPILER_FLAG("-mavx512vl" COMPILER_SUPPORT_AVX512VL)
    ENDIF()

    # Old GCC accepts unknown -Wno-* options without diagnostics unless another
    # warning is emitted. Probe the positive warning option instead.
    CHECK_C_COMPILER_FLAG("-Wstringop-overread" COMPILER_SUPPORT_WSTRINGOP_OVERREAD)
    CHECK_CXX_COMPILER_FLAG("-Wstringop-overread" COMPILER_SUPPORT_CXX_WSTRINGOP_OVERREAD)
    IF(COMPILER_SUPPORT_WSTRINGOP_OVERREAD)
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-stringop-overread")
    ENDIF()
    IF(COMPILER_SUPPORT_CXX_WSTRINGOP_OVERREAD)
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-stringop-overread")
    ENDIF()

    IF(COMPILER_SUPPORT_SSE42)
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4.2")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
    ENDIF()

    IF(SIMD_SUPPORT)
        IF(COMPILER_SUPPORT_FMA)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mfma")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mfma")
            MESSAGE(STATUS "FMA instructions is ACTIVATED")
        ENDIF()

        IF(COMPILER_SUPPORT_AVX)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
            MESSAGE(STATUS "AVX instructions is ACTIVATED")
        ENDIF()

        IF(COMPILER_SUPPORT_AVX2)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx2")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
            MESSAGE(STATUS "AVX2 instructions is ACTIVATED")
        ENDIF()
    ENDIF()

    IF(SIMD_AVX512_SUPPORT)
        IF(COMPILER_SUPPORT_AVX512F AND COMPILER_SUPPORT_AVX512BMI)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx512f -mavx512vbmi")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mavx512vbmi")
            MESSAGE(STATUS "avx512f/avx512bmi enabled by compiler")
        ENDIF()

        IF(COMPILER_SUPPORT_AVX512VL)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx512vl")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512vl")
            MESSAGE(STATUS "avx512vl enabled by compiler")
        ENDIF()
    ENDIF()

    IF(BUILD_SANITIZER)
        # NOTE: the security hardening flags in the TD_LINUX branch below
        #   (-fstack-protector-strong / -pie / -z relro -z now) are intentionally
        #   NOT applied here: BUILD_SANITIZER produces ASan debug/CI artifacts
        #   that are never shipped, so only production Linux builds harden.
        # Note: -fsanitize=undefined is intentionally omitted from C_FLAGS.
        # The manylinux2014 (CentOS 7) build container ships GCC 7 which generates
        # ubsan v0 ABI calls (e.g. __ubsan_handle_type_mismatch) but the only
        # available 64-bit libubsan (devtoolset-10) provides v1 symbols only
        # (__ubsan_handle_type_mismatch_v1), causing an unresolvable link error
        # with the mold linker.  ASan (-fsanitize=address) works correctly.
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}     -Werror -Werror=return-type -fPIC -gdwarf-2 -fsanitize=address -fsanitize-recover=all -fno-sanitize=shift-base -fno-sanitize=alignment -g3 -Wformat=0")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-literal-suffix -Werror=return-type -fPIC -gdwarf-2 -fsanitize=address -fsanitize-recover=all -fno-sanitize=shift-base -fno-sanitize=alignment -g3 -Wformat=0")
        IF(TD_DARWIN)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-deprecated-declarations")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
            MESSAGE(STATUS "macOS AddressSanitizer build: deprecated declaration warnings disabled")
        ENDIF()
        MESSAGE(STATUS "Compile with Address Sanitizer!")
    elseif(TD_LINUX)
        # ── Security hardening ────────────────────────────────────────────
        # Stack protection: -fstack-protector-strong inserts stack canaries in
        #   functions that have local buffers or address-taken variables.
        #   Probed first for C and C++ separately (same convention as
        #   -Wstringop-overread above) so an old/obscure toolchain without the
        #   option degrades gracefully instead of failing under -Werror.
        # PIE: objects are compiled -fPIC (position independent and a valid
        #   input for PIE executables); executables are linked with -pie below
        #   so shipped binaries are position-independent executables.
        #   NOTE: do NOT append -fPIE after -fPIC — the last switch wins, and
        #   -fPIE would degrade shared-library codegen (GOT/interposition).
        #   -pie is only applied when the toolchain supports it
        #   (include(CheckPIESupported) / check_pie_supported()) AND TD_LINUX_PIE
        #   is ON.  Turn -DTD_LINUX_PIE=OFF only when linking prebuilt non-PIC
        #   static archives that cannot go into a PIE executable.
        # RELRO: -z relro makes GOT/other relocations read-only after startup;
        #   -z now disables lazy binding => Full RELRO.
        CHECK_C_COMPILER_FLAG("-fstack-protector-strong" COMPILER_SUPPORT_STACK_PROTECTOR_STRONG)
        CHECK_CXX_COMPILER_FLAG("-fstack-protector-strong" COMPILER_SUPPORT_CXX_STACK_PROTECTOR_STRONG)
        IF(COMPILER_SUPPORT_STACK_PROTECTOR_STRONG)
            SET(_td_stack_protector_flags " -fstack-protector-strong")
        ELSE()
            SET(_td_stack_protector_flags "")
            MESSAGE(STATUS "C compiler lacks -fstack-protector-strong, stack protection disabled")
        ENDIF()
        IF(COMPILER_SUPPORT_CXX_STACK_PROTECTOR_STRONG)
            SET(_td_cxx_stack_protector_flags " -fstack-protector-strong")
        ELSE()
            SET(_td_cxx_stack_protector_flags "")
            MESSAGE(STATUS "C++ compiler lacks -fstack-protector-strong, stack protection disabled")
        ENDIF()
        OPTION(TD_LINUX_PIE "link Linux executables with -pie (turn OFF when linking non-PIC prebuilt static archives)" ON)
        INCLUDE(CheckPIESupported)
        # NOTE: plain OUTPUT_VARIABLE form only -- RESULT_VARIABLE is a newer
        #   CMake addition and must stay out for cmake_minimum_required(3.16).
        #   The probe stores its verdict in CMAKE_<LANG>_LINK_PIE_SUPPORTED.
        CHECK_PIE_SUPPORTED(OUTPUT_VARIABLE _td_pie_output)
        IF(CMAKE_C_LINK_PIE_SUPPORTED AND CMAKE_CXX_LINK_PIE_SUPPORTED)
            SET(_td_pie_supported TRUE)
        ELSE()
            SET(_td_pie_supported FALSE)
        ENDIF()
        IF(TD_LINUX_PIE AND _td_pie_supported)
            SET(_td_pie_flag " -pie")
        ELSE()
            SET(_td_pie_flag "")
            MESSAGE(STATUS "PIE executables disabled: TD_LINUX_PIE=${TD_LINUX_PIE}, toolchain PIE support=${_td_pie_supported}")
            MESSAGE(STATUS "check_pie_supported output: ${_td_pie_output}")
        ENDIF()
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror -Wformat=2 -Werror=format${_td_stack_protector_flags} -fPIC -g3 -gdwarf-2 -Wno-format-truncation -Wno-write-strings -Wno-format-overflow")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wformat=2 -Werror=format${_td_cxx_stack_protector_flags} -fPIC -g3 -gdwarf-2 -Wno-format-truncation -Wno-write-strings -Wno-format-overflow -Wno-conversion-null")
        # RELRO/BIND_NOW: probe linker support first (same convention as the
        #   -Wl,--icf=all probe in platform.cmake) so a non-GNU linker (some
        #   lld/mold/cross toolchains) degrades gracefully instead of failing
        #   the link with unrecognized options.  Probe the C and the C++ link
        #   driver: the flags land in CMAKE_EXE_LINKER_FLAGS and
        #   CMAKE_SHARED_LINKER_FLAGS, so they also reach C++ targets and C++
        #   link commands, which may use a different driver or extra defaults.
        INCLUDE(CheckCSourceCompiles)
        INCLUDE(CheckCXXSourceCompiles)
        # Save/restore instead of bare UNSET so later if(DEFINED) checks see the original state.
        SET(_td_required_link_options_was_defined FALSE)
        IF(DEFINED CMAKE_REQUIRED_LINK_OPTIONS)
            SET(_td_required_link_options_was_defined TRUE)
            SET(_td_saved_required_link_options "${CMAKE_REQUIRED_LINK_OPTIONS}")
        ENDIF()
        SET(CMAKE_REQUIRED_LINK_OPTIONS "-Wl,-z,relro" "-Wl,-z,now")
        check_c_source_compiles("int main(){return 0;}" LINKER_SUPPORTS_RELRO_NOW)
        check_cxx_source_compiles("int main(){return 0;}" LINKER_SUPPORTS_CXX_RELRO_NOW)
        IF(_td_required_link_options_was_defined)
            SET(CMAKE_REQUIRED_LINK_OPTIONS "${_td_saved_required_link_options}")
        ELSE()
            UNSET(CMAKE_REQUIRED_LINK_OPTIONS)
        ENDIF()
        IF(LINKER_SUPPORTS_RELRO_NOW AND LINKER_SUPPORTS_CXX_RELRO_NOW)
            SET(_td_relro_flags " -Wl,-z,relro -Wl,-z,now")
        ELSE()
            SET(_td_relro_flags "")
            MESSAGE(STATUS "Linker lacks -z relro/-z now (C: ${LINKER_SUPPORTS_RELRO_NOW}, C++: ${LINKER_SUPPORTS_CXX_RELRO_NOW}), Full RELRO disabled")
        ENDIF()
        SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}${_td_pie_flag}${_td_relro_flags}")
        SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}${_td_relro_flags}")
        MESSAGE(STATUS "Linux hardening enabled (C):${_td_stack_protector_flags}${_td_pie_flag}${_td_relro_flags}")
        MESSAGE(STATUS "Linux hardening enabled (C++):${_td_cxx_stack_protector_flags}${_td_pie_flag}${_td_relro_flags}")
    elseif(TD_DARWIN)
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror -Werror=return-type -fPIC -g3 -gdwarf-2 -Wformat=2 -Wno-format-nonliteral -Wno-format-y2k -Wno-deprecated-declarations")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Werror=return-type -fPIC -g3 -gdwarf-2 -Wno-reserved-user-defined-literal -Wformat=2 -Wno-format-nonliteral -Wno-format-y2k -Wno-deprecated-declarations -Wno-literal-conversion -Wno-writable-strings -Wno-unused-value -Wno-format -Wno-null-conversion")
    ELSE()
        message(FATAL_ERROR "not implemented yet")
    ENDIF()
ENDIF()

IF(TD_LINUX_64)
    # NOTE: need to test
    IF(BUILD_JEMALLOC)
        MESSAGE(STATUS "JEMALLOC Enabled")
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=attributes")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=attributes")
        SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
        ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib)
    ELSE()
        MESSAGE(STATUS "JEMALLOC Disabled")
        SET(LINK_JEMALLOC "")
    ENDIF()
ENDIF()

if(TD_DARWIN)
    function(td_fix_macos_client_libtaos_dep target_name)
        set_target_properties(${target_name} PROPERTIES
            MACOSX_RPATH TRUE
            BUILD_WITH_INSTALL_RPATH TRUE
            SKIP_BUILD_RPATH FALSE
            BUILD_RPATH "@loader_path;@loader_path/../driver;/usr/local/lib"
            INSTALL_RPATH "@loader_path;@loader_path/../driver;/usr/local/lib"
        )

        # CLI tools now link the native driver directly as well as the dispatcher.
        # Both libraries live in ../driver after installation, not beside the tool.
        # The recorded name is either @loader_path/<lib>.dylib or the absolute
        # build-tree path, depending on whether stage_runtime_libs.sh
        # fix-darwin-rpaths rewrote build/lib before this tool linked.
        foreach(client_lib IN ITEMS libtaos libtaosnative)
            add_custom_command(TARGET ${target_name} POST_BUILD
                COMMAND /bin/sh -c
                    "bin=\"$1\"; lib=\"$2\"; deps=\"$(otool -L \"$bin\")\" || exit 1; printf '%s' \"$deps\" | awk 'NR > 1 { sub(/^[[:space:]]+/, \"\"); sub(/[[:space:]]+[(]compatibility version.*$/, \"\"); print }' | while IFS= read -r dep; do case \"$dep\" in @rpath/*) ;; \"$lib.dylib\"|*/\"$lib.dylib\") install_name_tool -change \"$dep\" \"@rpath/$lib.dylib\" \"$bin\" || exit 1 ;; esac; done"
                    _
                    "$<TARGET_FILE:${target_name}>"
                    "${client_lib}"
                VERBATIM
            )
        endforeach()
    endfunction()
endif()
