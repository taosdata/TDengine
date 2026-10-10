# Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

aux_source_directory(src TRANSPORT_SRC)

IF(TD_ENTERPRISE)
	LIST(APPEND TRANSPORT_SRC ${TD_ENTERPRISE_DIR}/source/plugins/trans/src/transTLSImpl.c)
    LIST(APPEND TRANSPORT_SRC ${TD_ENTERPRISE_DIR}/source/plugins/trans/src/transSaslImpl.c)
ENDIF()

add_library(transport STATIC ${TRANSPORT_SRC})
target_include_directories(
  transport
  PUBLIC "${TD_SOURCE_DIR}/include/libs/transport"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)


target_link_libraries(
  transport
  PUBLIC os
  PUBLIC util
  PUBLIC common
)

if (NOT ${TD_WINDOWS})
  DEP_ext_ssl(transport)
endif()


if (${TD_LINUX})
  if (${TD_ENTERPRISE})        # {
    if(BUILD_LIBSASL)
      DEP_ext_sasl2(transport)
      target_compile_definitions(transport PRIVATE TD_BUILD_WITH_LIBSASL)
    endif()
  endif()
endif()

if(BUILD_WITH_UV_TRANS)
  if(BUILD_WITH_UV)
    DEP_ext_libuv(transport)
    if(BUILD_PTHREAD_TWEAK)       # {
      target_link_libraries(transport PRIVATE pthread)
    endif()                    # }
  endif()
endif()

if(BUILD_WITH_UV_TRANS)
  if(BUILD_WITH_UV)
    add_definitions(-DUSE_UV)
  endif()
endif()


# if(${BUILD_TEST})
#     add_subdirectory(test)
# endif(${BUILD_TEST})

if(BUILD_TEST AND TD_LINUX)
  add_executable(transportTest test/transportTests.cpp test/transportHeartbeatHarness.c
                 test/transSvrUserInfoHarness.c)
  DEP_ext_gtest(transportTest)
  target_include_directories(transportTest PRIVATE $<TARGET_PROPERTY:transport,INCLUDE_DIRECTORIES>)
  target_compile_definitions(transportTest PRIVATE $<TARGET_PROPERTY:transport,COMPILE_DEFINITIONS>)
  target_link_libraries(transportTest PRIVATE os util common transport function)
  if(BUILD_ASTRA_RPC)
    target_link_options(transportTest PRIVATE -Wl,--wrap=rpcFreeCont)
  endif()
  add_test(NAME transportTest COMMAND transportTest)
endif()
