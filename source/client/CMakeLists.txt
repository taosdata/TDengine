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

aux_source_directory(src CLIENT_SRC)

if(TD_ENTERPRISE)
    LIST(APPEND CLIENT_SRC ${TD_ENTERPRISE_DIR}/source/plugins/view/src/clientView.c)
endif()

if(TD_WINDOWS)
    configure_file(src/taosnative.rc.in "${CMAKE_CURRENT_BINARY_DIR}/taosnative.rc")
    add_library(${TAOS_NATIVE_LIB} SHARED ${CLIENT_SRC} "${CMAKE_CURRENT_BINARY_DIR}/taosnative.rc")
    # The dispatcher reads tdNativeApiTable out of this driver with
    # GetProcAddress.  Windows exports only symbols marked __declspec(dllexport),
    # and a data symbol gets no benefit from the public headers, so the table needs
    # the attribute explicitly -- see source/client/inc/taosNativeApi.h.  Only the
    # shared driver carries it; the static variant is linked into tests that do not
    # look the table up.
    target_compile_definitions(${TAOS_NATIVE_LIB} PRIVATE TD_NATIVE_API_TABLE_EXPORTS)
else()
    add_library(${TAOS_NATIVE_LIB} SHARED ${CLIENT_SRC})
endif()

if(TD_DARWIN)
    target_compile_options(${TAOS_NATIVE_LIB} PRIVATE -Wno-error=deprecated-non-prototype)
endif()

# Export convergence for libtaosnative.so: hide the engine/third-party symbols
# that no in-tree consumer imports, which also lets --gc-sections drop the code
# behind them.
#
# This is only safe because libtaosnative.so is the *single* provider of these
# symbols: libtaos.so is a thin dispatcher and no longer embeds the engine, so
# there is no second copy whose state could diverge.  The earlier attempt hid
# them in both libraries, which split tsNumOfCores between the two copies and
# made every client fail config init with TSDB_CODE_OUT_OF_RANGE.
#
# See taosnative.ver; regenerate it whenever the consumer set changes.
if(TD_LINUX)
    target_link_options(${TAOS_NATIVE_LIB} PRIVATE
        "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/taosnative.ver"
        # The CLI tools call these util/os helpers, but nothing inside the engine
        # does, so the archive members defining them would never be pulled in and
        # no consumer could resolve them.  Force those members in rather than let
        # every tool embed its own copy of util/os (and of their state).
        #
        # base58/taosDes* are the same case one level down: the enterprise
        # license archive (libgrant) is a single prebuilt machine_*.o, so pulling
        # it in for tGetMachineId() also drags in the activation-code helpers,
        # which reference these four -- nothing else does.
        "LINKER:-u,taosCreateThread"
        "LINKER:-u,taosDestroyThread"
        "LINKER:-u,taosThreadRunning"
        "LINKER:-u,taosSetSignal"
        "LINKER:-u,taosIgnSignal"
        "LINKER:-u,base58_decode"
        "LINKER:-u,base58_encode"
        "LINKER:-u,taosDesDecode"
        "LINKER:-u,taosDesEncode"
        # Same case again for the terminal-mode helpers: only the shell and
        # taosdump call them.  osSystem.c.o currently lands in the library by
        # accident -- nothing in the engine references it either, and it is
        # pulled in for taosOpenCmd through the crash-report path (tlog.c ->
        # tmisce.c -> osSysinfo.c).  In a configuration where that chain is
        # absent the member is never archived in, and the tools fail to link
        # with exactly these four undefined symbols.  Force it in instead of
        # depending on that accident.
        "LINKER:-u,taosSetConsoleEcho"
        "LINKER:-u,taosSetTerminalMode"
        "LINKER:-u,taosGetOldTerminalMode"
        "LINKER:-u,taosResetTerminalMode")

    # A version script is not a link dependency: without this, editing
    # taosnative.ver does not relink the library and the change looks like it had
    # no effect (the stale export table keeps being used).
    set_property(TARGET ${TAOS_NATIVE_LIB} APPEND PROPERTY LINK_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/taosnative.ver")
elseif(TD_DARWIN)
    # Mach-O has no version script, so macOS currently gets the thin dispatcher
    # without export convergence (the ld64 equivalent, -exported_symbols_list, is
    # a separate generated artifact and needs a macOS validation run).  The same
    # helpers still have to be forced in: nothing inside the engine references
    # them, so the archive members would otherwise never be pulled in and the CLI
    # tools could not resolve them.  ld64 -u takes the Mach-O symbol form, i.e.
    # with the leading underscore.
    target_link_options(${TAOS_NATIVE_LIB} PRIVATE
        "LINKER:-u,_taosCreateThread"
        "LINKER:-u,_taosDestroyThread"
        "LINKER:-u,_taosThreadRunning"
        "LINKER:-u,_taosSetSignal"
        "LINKER:-u,_taosIgnSignal"
        # See the Linux branch above: the shell/taosdump are the only users of
        # the terminal-mode helpers, so force osSystem.o in rather than rely on
        # an indirect reference to pull it in.
        "LINKER:-u,_taosSetConsoleEcho"
        "LINKER:-u,_taosSetTerminalMode"
        "LINKER:-u,_taosGetOldTerminalMode"
        "LINKER:-u,_taosResetTerminalMode"
        # See the Linux branch above: libgrant's machine_*.o needs these.
        "LINKER:-u,_base58_decode"
        "LINKER:-u,_base58_encode"
        "LINKER:-u,_taosDesDecode"
        "LINKER:-u,_taosDesEncode")
endif()


message(STATUS "Debugging ............................")
message(STATUS "TAOS_NATIVE_LIB:${TAOS_NATIVE_LIB}")
message(STATUS "TD_SOURCE_DIR:${TD_SOURCE_DIR}")


target_include_directories(
    ${TAOS_NATIVE_LIB}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
set(TAOSD_MODULE "")
if(BUILD_TAOSD_INTEGRATED)
    set(TAOSD_MODULE "taosd")
endif()
target_link_libraries(
    ${TAOS_NATIVE_LIB}
    INTERFACE api
    PRIVATE util common transport monitor nodes parser command planner catalog scheduler function qcom geometry ${TAOSD_MODULE} decimal extconnector
    # PRIVATE, not PUBLIC: the driver is a shared library, so it does not need to
    # hand os's archive to its consumers.  With PUBLIC, libos.a lands on every
    # consumer's link line (taosdump, taos, taosBenchmark, the C test tools...)
    # and, now that the version script hides part of the engine here, the linker
    # pulls its members into the executable instead of resolving dynamically --
    # the executable then carries a second osDefaultInit/tsNumOfCores.  Harmless
    # only as long as the copy stays private; tmq_sim exported it and became the
    # provider for libtaosnative.so, which split the engine's global state and
    # made taos_connect() return NULL.
    PRIVATE os
)

# os's include directories still have to reach consumers (they compile against
# os.h), but only those -- see the PRIVATE comment above.
target_include_directories(
    ${TAOS_NATIVE_LIB}
    INTERFACE
    "${TD_SOURCE_DIR}/include/os"
    "${TD_SOURCE_DIR}/include"
    "${TD_SOURCE_DIR}/include/util"
)

add_library(${TAOS_NATIVE_LIB_STATIC} STATIC ${CLIENT_SRC})

if(TD_DARWIN)
    target_compile_options(${TAOS_NATIVE_LIB_STATIC} PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    ${TAOS_NATIVE_LIB_STATIC}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)
target_link_libraries(
    ${TAOS_NATIVE_LIB_STATIC}
    INTERFACE api
    PUBLIC os util common transport monitor nodes parser command planner catalog scheduler function qcom geometry decimal
)

# libgsasl: application-layer SCRAM-SHA-256 authentication on the CONNECT handshake
if(${BUILD_LIBGSASL} AND ${TD_LINUX})
    DEP_ext_gsasl(${TAOS_NATIVE_LIB})
    DEP_ext_gsasl(${TAOS_NATIVE_LIB_STATIC})
    add_definitions(-DUSE_LIBGSASL)
endif()

if(BUILD_TEST)
    ADD_SUBDIRECTORY(test)
endif()

ADD_SUBDIRECTORY(wrapper)
