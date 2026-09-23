# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

add_executable(xnodeEncodeDecodeTest "xnodeEncodeDecodeTest.cpp")
DEP_ext_gtest(xnodeEncodeDecodeTest)
target_include_directories(
  xnodeEncodeDecodeTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
)
target_link_libraries(
  xnodeEncodeDecodeTest
  PUBLIC sut
  sdb
)

add_test(
  NAME xnodeEncodeDecodeTest
  COMMAND xnodeEncodeDecodeTest
)

# Simplified standalone test
add_executable(xnodeSimpleTest "xnodeSimpleTest.cpp")
DEP_ext_gtest(xnodeSimpleTest)
target_include_directories(
  xnodeSimpleTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
)

target_link_libraries(
  xnodeSimpleTest
  PUBLIC
)

add_test(
  NAME xnodeSimpleTest
  COMMAND xnodeSimpleTest
)

# Memory management tests
add_executable(xnodeMemoryTest "xnodeMemoryTest.cpp")
DEP_ext_gtest(xnodeMemoryTest)
target_include_directories(
  xnodeMemoryTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/dnode/mnode"
  "${TD_SOURCE_DIR}/include/dnode/mgmt"
  "${TD_SOURCE_DIR}/include/dnode/xnode"
  "${TD_SOURCE_DIR}/include/dnode/impl/inc"
  "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
)
target_link_libraries(
  xnodeMemoryTest
  PUBLIC
  mnode xnode mgmt_xnode os util
)

add_test(
  NAME xnodeMemoryTest
  COMMAND xnodeMemoryTest
)

# Test real XNode utility functions (completely independent, no TDengine headers)
add_executable(xnodeRealFunctionsTest "xnodeRealFunctionsTest.cpp")
DEP_ext_gtest(xnodeRealFunctionsTest)
target_link_libraries(xnodeRealFunctionsTest
  PUBLIC
)

add_test(
  NAME xnodeRealFunctionsTest
  COMMAND xnodeRealFunctionsTest
)

# SDB operations tests (encode/decode, insert/update/delete)
# Uses mock implementations that mirror production code logic
add_executable(xnodeSdbTest "xnodeSdbTest.cpp")
DEP_ext_gtest(xnodeSdbTest)
target_include_directories(
  xnodeSdbTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/dnode/mnode"
  "${TD_SOURCE_DIR}/include/dnode/mgmt"
  "${TD_SOURCE_DIR}/include/dnode/xnode"
  "${TD_SOURCE_DIR}/include/dnode/impl/inc"
  "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
)

target_link_libraries(xnodeSdbTest
  PUBLIC
  mnode xnode txnode os util
)

add_test(
  NAME xnodeSdbTest
  COMMAND xnodeSdbTest
)

# Production code tests - uses REAL encode/decode functions from mndXnode.c
add_executable(xnodeProductionTest "xnodeProductionTest.cpp")
DEP_ext_gtest(xnodeProductionTest)
target_include_directories(
  xnodeProductionTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
  "${TD_SOURCE_DIR}/include/dnode/mnode"
  "${TD_SOURCE_DIR}/include/dnode/mgmt"
  "${TD_SOURCE_DIR}/include/dnode/xnode"
  "${TD_SOURCE_DIR}/include/dnode/impl/inc"
  "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
)

target_link_libraries(xnodeProductionTest
  PUBLIC
  mnode
  xnode
  mgmt_xnode
)

add_test(
  NAME xnodeProductionTest
  COMMAND xnodeProductionTest
)

# Agent code tests - uses REAL agent functions from mndXnode.c
add_executable(xnodeAgentTest "xnodeAgentTest.cpp")
DEP_ext_gtest(xnodeAgentTest)
target_include_directories(
  xnodeAgentTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
  "${TD_SOURCE_DIR}/include/dnode/mnode"
  "${TD_SOURCE_DIR}/include/dnode/mgmt"
  "${TD_SOURCE_DIR}/include/dnode/xnode"
  "${TD_SOURCE_DIR}/include/dnode/impl/inc"
  "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
)

target_link_libraries(xnodeAgentTest
  PUBLIC
  mnode
  xnode
  mgmt_xnode
)

add_test(
  NAME xnodeAgentTest
  COMMAND xnodeAgentTest
)

# Show protection tests - uses REAL show protection functions from mndXnode.c
add_executable(xnodeShowProtectionTest "xnodeShowProtectionTest.cpp")
DEP_ext_gtest(xnodeShowProtectionTest)
target_include_directories(
  xnodeShowProtectionTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
  "${TD_SOURCE_DIR}/include/dnode/mnode"
  "${TD_SOURCE_DIR}/include/dnode/mgmt"
  "${TD_SOURCE_DIR}/include/dnode/xnode"
  "${TD_SOURCE_DIR}/include/dnode/impl/inc"
  "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
)
target_link_libraries(
  xnodeShowProtectionTest
  PUBLIC
  mnode
  xnode
  mgmt_xnode
)
add_test(
  NAME xnodeShowProtectionTest
  COMMAND xnodeShowProtectionTest
)

# Six-star write-semantics tests - executes the shared T1a merge-semantics
# fixture against the production scan/merge implementation in mndXnode.c.
set(
  XNODE_SIXSTAR_FIXTURE_LOCAL_PATH
  "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/xnode-six-star-merge-semantics.json"
)
if(NOT EXISTS "${XNODE_SIXSTAR_FIXTURE_LOCAL_PATH}")
  message(FATAL_ERROR
          "xnodeSixStarWriteTest fixture is missing at ${XNODE_SIXSTAR_FIXTURE_LOCAL_PATH}")
endif()
set(
  XNODE_SIXSTAR_FIXTURE_PATH
  "${CMAKE_CURRENT_BINARY_DIR}/xnode-six-star-merge-semantics.json"
)
configure_file(
  "${XNODE_SIXSTAR_FIXTURE_LOCAL_PATH}"
  "${XNODE_SIXSTAR_FIXTURE_PATH}"
  COPYONLY
)

add_executable(xnodeSixStarWriteTest "xnodeSixStarWriteTest.cpp")
DEP_ext_gtest(xnodeSixStarWriteTest)
target_include_directories(
  xnodeSixStarWriteTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
  "${TD_SOURCE_DIR}/include/dnode/mnode"
  "${TD_SOURCE_DIR}/include/dnode/mgmt"
  "${TD_SOURCE_DIR}/include/dnode/xnode"
  "${TD_SOURCE_DIR}/include/dnode/impl/inc"
  "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
)
target_compile_definitions(
  xnodeSixStarWriteTest
  PRIVATE XNODE_SIXSTAR_FIXTURE_PATH="${XNODE_SIXSTAR_FIXTURE_PATH}"
)
target_link_libraries(
  xnodeSixStarWriteTest
  PUBLIC
  mnode
  xnode
  mgmt_xnode
)
add_test(
  NAME xnodeSixStarWriteTest
  COMMAND xnodeSixStarWriteTest
)

if(TD_WINDOWS)
  message(STATUS "Skipping envelopeCryptoTest: Windows does not support OpenSSL")
else()
  include(CheckCSourceCompiles)

  set(_envelope_crypto_previous_try_compile_target_type
      "${CMAKE_TRY_COMPILE_TARGET_TYPE}")
  if(DEFINED CMAKE_TRY_COMPILE_TARGET_TYPE)
    set(_envelope_crypto_try_compile_target_type_was_defined TRUE)
  else()
    set(_envelope_crypto_try_compile_target_type_was_defined FALSE)
  endif()
  if(DEFINED CMAKE_REQUIRED_INCLUDES)
    set(_envelope_crypto_required_includes_was_defined TRUE)
  else()
    set(_envelope_crypto_required_includes_was_defined FALSE)
  endif()
  set(_envelope_crypto_previous_required_includes
      "${CMAKE_REQUIRED_INCLUDES}")
  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
  set(CMAKE_REQUIRED_INCLUDES "${TD_SOURCE_DIR}/include/libs/crypt")
  unset(ENVELOPE_CRYPTO_HAS_PUBLIC_FIELD_PRIMITIVE CACHE)
  check_c_source_compiles(
    "#include <envelope_crypto.h>\nint main(void) { (void)&envelopeEncrypt; return 0; }"
    ENVELOPE_CRYPTO_HAS_PUBLIC_FIELD_PRIMITIVE
  )
  if(_envelope_crypto_try_compile_target_type_was_defined)
    set(CMAKE_TRY_COMPILE_TARGET_TYPE
        "${_envelope_crypto_previous_try_compile_target_type}")
  else()
    unset(CMAKE_TRY_COMPILE_TARGET_TYPE)
  endif()
  if(_envelope_crypto_required_includes_was_defined)
    set(CMAKE_REQUIRED_INCLUDES
        "${_envelope_crypto_previous_required_includes}")
  else()
    unset(CMAKE_REQUIRED_INCLUDES)
  endif()
  unset(_envelope_crypto_required_includes_was_defined)
  unset(_envelope_crypto_previous_required_includes)
  unset(_envelope_crypto_try_compile_target_type_was_defined)
  unset(_envelope_crypto_previous_try_compile_target_type)
  if(ENVELOPE_CRYPTO_HAS_PUBLIC_FIELD_PRIMITIVE)
    message(FATAL_ERROR
            "envelope_crypto.h must not expose envelopeEncrypt")
  endif()
  unset(ENVELOPE_CRYPTO_HAS_PUBLIC_FIELD_PRIMITIVE CACHE)

  set(
    ENVELOPE_CRYPTO_FIXTURE_LOCAL_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/xnode-dsn-v1-fixtures.json"
  )
  if(NOT EXISTS "${ENVELOPE_CRYPTO_FIXTURE_LOCAL_PATH}")
    message(FATAL_ERROR
            "envelopeCryptoTest fixture is missing at ${ENVELOPE_CRYPTO_FIXTURE_LOCAL_PATH}")
  endif()
  set(
    ENVELOPE_CRYPTO_FIXTURE_PATH
    "${CMAKE_CURRENT_BINARY_DIR}/xnode-dsn-v1-fixtures.json"
  )
  configure_file(
    "${ENVELOPE_CRYPTO_FIXTURE_LOCAL_PATH}"
    "${ENVELOPE_CRYPTO_FIXTURE_PATH}"
    COPYONLY
  )

  add_executable(envelopeCryptoTest "envelopeCryptoTest.cpp")
  DEP_ext_gtest(envelopeCryptoTest)
  DEP_ext_ssl(envelopeCryptoTest)
  target_compile_definitions(
    envelopeCryptoTest
    PRIVATE ENVELOPE_CRYPTO_FIXTURE_PATH="${ENVELOPE_CRYPTO_FIXTURE_PATH}"
  )
  target_link_libraries(
    envelopeCryptoTest
    PUBLIC crypt util os
  )

  add_test(
    NAME envelopeCryptoTest
    COMMAND envelopeCryptoTest
  )
endif()
