# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

if(NOT BUILD_MIMALLOC)
  return()
endif()
if(NOT TD_MIMALLOC_SUPPORTED)
  message(FATAL_ERROR "BUILD_MIMALLOC requires Windows x64/MSVC, without ASan or BUILD_TAOSD_INTEGRATED; set BUILD_MIMALLOC=OFF")
endif()
if(CMAKE_VERSION VERSION_LESS 3.18)
  message(FATAL_ERROR "mimalloc requires CMake 3.18 or newer")
endif()

set(_td_mi_version "3.6.1")
set(_td_mi_archive "mimalloc-${_td_mi_version}.tar.gz")
set(_td_mi_url "https://codeload.github.com/microsoft/mimalloc/tar.gz/refs/tags/v${_td_mi_version}")
# This file is shared by the Conan and ExternalProject entry paths.
if(NOT BUILD_USE_PUBLIC_DEPS)
  if(LOCAL_URL)
    set(_td_mi_url "${LOCAL_URL}/${_td_mi_archive}")
  elseif(BUILD_DEPS_MIRROR_URL)
    set(_td_mi_url "${BUILD_DEPS_MIRROR_URL}/${_td_mi_archive}")
  endif()
endif()
set(TD_EXTERNALS_BASE_DIR "${CMAKE_SOURCE_DIR}/.externals" CACHE PATH "path where external dependencies reside")
set(_td_mi_config "$<IF:$<BOOL:$<CONFIG>>,$<CONFIG>,Debug>")
set(_td_mi_root "${TD_EXTERNALS_BASE_DIR}/install/ext_mimalloc/${_td_mi_version}")
set(TD_MIMALLOC_INSTALL "${_td_mi_root}/${_td_mi_config}")
set(TD_MIMALLOC_NAME "mimalloc$<$<CONFIG:Debug>:-debug>")
if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(TD_MIMALLOC_NAME "mimalloc-debug")
endif()
set(TD_MIMALLOC_RUNTIME_FILES
  "${TD_MIMALLOC_INSTALL}/bin/${TD_MIMALLOC_NAME}.dll"
  "${TD_MIMALLOC_INSTALL}/bin/mimalloc-redirect.dll"
  "${TD_MIMALLOC_INSTALL}/bin/minject.exe")

if(BUILD_CONTRIB)
  include(ExternalProject)
  set(_td_mi_build_type "${CMAKE_BUILD_TYPE}")
  if(NOT _td_mi_build_type)
    set(_td_mi_build_type Debug)
  endif()
  ExternalProject_Add(ext_mimalloc
    PREFIX "${TD_EXTERNALS_BASE_DIR}/build/${CMAKE_BUILD_TYPE}/ext_mimalloc-${_td_mi_version}"
    URL "${_td_mi_url}"
    DOWNLOAD_NAME "${_td_mi_archive}"
    URL_HASH SHA256=2672e6a99d4579fb4bb32d9de0d63aa424ce600d31fbfaf06eb74d940d2e07a6
    CMAKE_ARGS
      "-DCMAKE_BUILD_TYPE=${_td_mi_build_type}"
      "-DCMAKE_INSTALL_PREFIX=${_td_mi_root}"
      "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
      "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
      -DMI_BUILD_SHARED=ON -DMI_BUILD_STATIC=OFF -DMI_BUILD_OBJECT=OFF -DMI_BUILD_TESTS=OFF
      -DMI_OVERRIDE=ON -DMI_WIN_REDIRECT=ON -DMI_INSTALL_TOPLEVEL=ON -DMI_OPT_ARCH=OFF
      -DMI_XMALLOC=OFF
    INSTALL_COMMAND ${CMAKE_COMMAND} --install <BINARY_DIR> --config ${_td_mi_config} --prefix "${TD_MIMALLOC_INSTALL}"
      COMMAND ${CMAKE_COMMAND} -E make_directory "${TD_MIMALLOC_INSTALL}/share/mimalloc"
      COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/bin/minject.exe "${TD_MIMALLOC_INSTALL}/bin/minject.exe"
      COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/LICENSE "${TD_MIMALLOC_INSTALL}/share/mimalloc/LICENSE")
  # ExternalProject can retain an install stamp from an older install layout.
  # Invalidate only that step when the selected cache is incomplete, preserving
  # the downloaded sources and compiled dependency. Never repair prebuilt-only
  # (BUILD_CONTRIB=OFF) caches implicitly.
  ExternalProject_Get_Property(ext_mimalloc STAMP_DIR)
  set(_td_mi_configs "${CMAKE_BUILD_TYPE}")
  if(CMAKE_CONFIGURATION_TYPES)
    set(_td_mi_configs ${CMAKE_CONFIGURATION_TYPES})
  elseif(NOT _td_mi_configs)
    set(_td_mi_configs Debug)
  endif()
  foreach(_td_mi_cache_config IN LISTS _td_mi_configs)
    set(_td_mi_cache_name mimalloc)
    if(_td_mi_cache_config STREQUAL "Debug")
      set(_td_mi_cache_name mimalloc-debug)
    endif()
    foreach(_td_mi_file include/mimalloc.h bin/${_td_mi_cache_name}.dll bin/mimalloc-redirect.dll bin/minject.exe share/mimalloc/LICENSE)
      if(NOT EXISTS "${_td_mi_root}/${_td_mi_cache_config}/${_td_mi_file}")
        set(_td_mi_install_stamp "${STAMP_DIR}/ext_mimalloc-install")
        if(CMAKE_CONFIGURATION_TYPES)
          set(_td_mi_install_stamp "${STAMP_DIR}/${_td_mi_cache_config}/ext_mimalloc-install")
        endif()
        if(EXISTS "${_td_mi_install_stamp}")
          message(STATUS "[mimalloc] Incomplete ${_td_mi_cache_config} cache (${_td_mi_file} missing); scheduling dependency reinstall")
          file(REMOVE "${_td_mi_install_stamp}")
        endif()
        break()
      endif()
    endforeach()
  endforeach()
  if(TARGET build_externals)
    add_dependencies(build_externals ext_mimalloc)
  endif()
endif()

# Ship the optional allocator and its upstream tool without linking or patching taosd.
# Validate at build time so multi-config generators only require the selected cache.
add_custom_target(stage_mimalloc ALL
  COMMAND ${CMAKE_COMMAND}
    "-DTD_MIMALLOC_INSTALL=${TD_MIMALLOC_INSTALL}" "-DTD_MIMALLOC_NAME=${TD_MIMALLOC_NAME}"
    -P "${CMAKE_CURRENT_LIST_DIR}/mimalloc-validate.cmake"
  COMMAND ${CMAKE_COMMAND} -E copy_if_different ${TD_MIMALLOC_RUNTIME_FILES} "$<TARGET_FILE_DIR:taosd>"
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
    "${TD_MIMALLOC_INSTALL}/share/mimalloc/LICENSE" "$<TARGET_FILE_DIR:taosd>/mimalloc-LICENSE.txt"
  DEPENDS taosd
  VERBATIM)
if(TARGET ext_mimalloc)
  # The DLLs are installed by ExternalProject before staging. File byproducts
  # with configuration expressions would require CMake 3.20 instead of 3.18.
  add_dependencies(stage_mimalloc ext_mimalloc)
endif()
