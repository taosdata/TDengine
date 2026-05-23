cmake_minimum_required(VERSION 3.14)

# ── Common include paths used by all taospyudf targets ───────────────────
set(_pyudf_common_incs
    src
    "${TD_SOURCE_DIR}/include/libs/function"
    "${TD_SOURCE_DIR}/include/client"
    "${TD_SOURCE_DIR}/include/util"
)

# ── Helper: configure one taospyudf shared-library target ────────────────
# Args: _tgt=target_name, _py_inc=Python include dir, _py_lib=import lib (Windows), _ver_short=3.14, _output_name=DLL name
macro(_pyudf_add_target _tgt _py_inc _py_lib _ver_short _output_name)
    set(_pyudf_py_version "${_ver_short}")  # for rc.in configure_file

    if(TD_WINDOWS)
        set(Python3_VERSION "${_pyudf_py_version}")
        configure_file(src/taospyudf.rc.in "${CMAKE_CURRENT_BINARY_DIR}/${_tgt}.rc")
        add_library(${_tgt} SHARED src/taospyudf.cpp "${CMAKE_CURRENT_BINARY_DIR}/${_tgt}.rc")
    else()
        add_library(${_tgt} SHARED src/taospyudf.cpp)
    endif()

    target_include_directories(${_tgt} PRIVATE ${_pyudf_common_incs} "${_py_inc}")

    DEP_ext_plog_INC(${_tgt})
    if(TARGET ext_plog)
        add_dependencies(${_tgt} ext_plog)
    endif()

    if(TD_WINDOWS)
        target_link_libraries(${_tgt} PRIVATE "${_py_lib}")
    else()
        if(APPLE)
            target_link_options(${_tgt} PRIVATE "LINKER:-undefined,dynamic_lookup")
        elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            target_link_options(${_tgt} PRIVATE "LINKER:--allow-shlib-undefined")
        endif()
    endif()

    target_compile_features(${_tgt} PRIVATE cxx_std_17)
    target_compile_definitions(${_tgt} PRIVATE BUILDING_DLL Py_LIMITED_API=0x030A0000)

    if(TD_WINDOWS)
        # Py_NO_LINK_LIB: disable #pragma comment(lib,...) auto-linking in pyconfig.h
        # — CMake handles linking explicitly.  Without this, Debug builds try to
        #   link python3XX_d.lib which doesn't exist in python-build-standalone.
        target_compile_definitions(${_tgt} PRIVATE Py_NO_LINK_LIB)
    else()
        target_compile_definitions(${_tgt} PRIVATE _GNU_SOURCE)
    endif()

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${_tgt} PRIVATE -Wno-attributes -Wno-deprecated-declarations)
    elseif(MSVC)
        target_compile_options(${_tgt} PRIVATE /EHsc)
        # NOTE: do NOT override MSVC_RUNTIME_LIBRARY here.
        # taosudf.exe uses the default CRT for the build type (Debug → /MDd,
        # Release → /MD).  The inline helpers in taosudf.h (udfColDataSet,
        # udfColEnsureCapacity) call realloc() on buffers originally allocated
        # by taosudf.  If this DLL uses a different CRT, the cross-heap
        # realloc corrupts memory and hangs on varchar columns.
        # Keep CRT consistent with taosudf to avoid cross-heap issues in
        # embedded CPython C API (Limited API / abi3) code paths.
    endif()

    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.0")
        target_link_libraries(${_tgt} PRIVATE stdc++fs)
    endif()

    target_link_libraries(${_tgt} PRIVATE ${CMAKE_DL_LIBS})

    if(TD_WINDOWS)
        set_target_properties(${_tgt} PROPERTIES
            OUTPUT_NAME "${_output_name}"
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/bin"
            ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib"
            PREFIX ""
        )
    else()
        set_target_properties(${_tgt} PROPERTIES
            OUTPUT_NAME "${_output_name}"
            LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib"
            PREFIX "lib"
            NO_SONAME ON
        )
    endif()
endmacro()

# ═══════════════════════════════════════════════════════════════════════════
# Python SDK downloaded via ExternalProject (PYUDF_CPYTHON_TARGET set by external.cmake)
# ═══════════════════════════════════════════════════════════════════════════
if(NOT PYUDF_CPYTHON_TARGET)
    message(FATAL_ERROR "[pyudf] PYUDF_CPYTHON_TARGET is empty. "
    "Ensure BUILD_PYUDF_PYTHON_VERSION is set when BUILD_PYUDF=ON.")
endif()

set(_cptarget "${PYUDF_CPYTHON_TARGET}")

set(_ver_short "${${_cptarget}_ver_short}")
set(_py_inc    "${${_cptarget}_inc_dir}")
set(_py_lib    "${${_cptarget}_lib_path}")

set(_tgt "taospyudf")
set(_oname "taospyudf")

_pyudf_add_target(${_tgt} "${_py_inc}" "${_py_lib}" "${_ver_short}" "${_oname}")
add_dependencies(${_tgt} ${_cptarget})

message(STATUS "[pyudf] target ${_tgt} → Python ${_ver_short} (single-library abi3 rollout mode)")
