# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

foreach(_file include/mimalloc.h bin/${TD_MIMALLOC_NAME}.dll bin/mimalloc-redirect.dll bin/minject.exe share/mimalloc/LICENSE)
  if(NOT EXISTS "${TD_MIMALLOC_INSTALL}/${_file}")
    message(FATAL_ERROR "Missing mimalloc package file: ${TD_MIMALLOC_INSTALL}/${_file}. Build with BUILD_CONTRIB=ON first, or use BUILD_MIMALLOC=OFF.")
  endif()
endforeach()
file(STRINGS "${TD_MIMALLOC_INSTALL}/include/mimalloc.h" _version REGEX "^#define MI_MALLOC_VERSION +30601([ \t]|$)")
if(NOT _version)
  message(FATAL_ERROR "mimalloc header version does not match 3.6.1: ${TD_MIMALLOC_INSTALL}")
endif()
