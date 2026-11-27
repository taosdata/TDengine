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

# Unit-test seams (include/util/tdTestSeam.h).  Tests inject a failure into an
# engine function by assigning a seam pointer instead of patching code, so the
# injection also reaches the engine's own call sites when the engine lives in a
# DSO.  Only test builds carry the seam lines; product binaries are unchanged.
if(BUILD_TEST)
    add_compile_definitions(BUILD_TEST_SEAM)
endif()

# Shared server engine for unit tests (BUILD_TEST_SHARED_ENGINE).
#
# Unit tests that exercise the storage/mnode code currently link the whole
# engine statically, so every test binary carries its own copy of the engine
# code and its DWARF (300+ MiB per test in a Debug build).  When this option is
# enabled a single libtaosd_shared.so is built from the same libraries taosd
# uses, and the test executables resolve engine symbols from it instead of
# pulling the static archives.
#
# The option is off by default; only the CI unit-test build enables it.
#
# Some tests must keep the static linkage:
#   * targets linked with -Wl,--wrap=... : --wrap only rewrites unresolved
#     references at link time, an engine function that already lives in the
#     DSO cannot be wrapped;
#   * targets that use cppstub to replace a function the engine itself calls:
#     cppstub rewrites the function entry at run time, which only intercepts
#     the call sites inside the executable, not the ones already compiled into
#     the DSO.
#   * targets that use no engine code at all (a pure third-party-library
#     regression test, for example): they link neither the engine nor the
#     prebuilt libs3.a, so the DSO's intentionally undefined S3_* entry points
#     cannot be resolved and, because both the DSO and the test are linked with
#     -z now, ld.so aborts with "undefined symbol: S3_initialize" before main().
# Those groups are listed in TD_TEST_SHARED_ENGINE_SKIP.

set(TD_TEST_SHARED_ENGINE_LIBS
    os
    util
    common
    aes
    audit
    bnode
    catalog
    command
    crypt
    decimal
    dmodule
    dnode
    executor
    extconnector
    function
    geometry
    grant
    index
    metrics
    mgmt_bnode
    mgmt_dnode
    mgmt_mnode
    mgmt_qnode
    mgmt_snode
    mgmt_vnode
    mgmt_xnode
    mnode
    monitor
    monitorfw
    new-stream
    nodes
    node_util
    parser
    planner
    qcom
    qnode
    qworker
    scalar
    scheduler
    sdb
    sm4
    snode
    storage
    sync
    taosnative_static
    taosk
    tdb
    tfs
    tmqtt_mgmt
    totp
    transport
    tss
    TSZ
    txnode
    vnode
    wal
    wideInteger
    wlr
    xnode
)

# common/taosk reference S3/COS globals that are only defined by the prebuilt,
# non-PIC libs3.a; whole-archiving them would make the DSO unlinkable.  They
# are linked normally so only the members the engine really needs are pulled.
set(TD_TEST_SHARED_ENGINE_WHOLE "${TD_TEST_SHARED_ENGINE_LIBS}")
list(REMOVE_ITEM TD_TEST_SHARED_ENGINE_WHOLE common taosk)

set(TD_TEST_SHARED_ENGINE_SKIP
    # cppstub, but the stubbed function is called from inside the engine: the
    # patch misses the DSO's own call sites, so the stub silently does nothing
    # (streamNotifyTest expects gCurlWsSendCalls == 1 and gets 0).  They pass
    # when linked statically and crash against the shared engine.  The other
    # cppstub tests do work with the shared engine -- tsdbReaderGroupSwitchTest
    # 230/230, tsdbReaderResetTest 12/12, execUtilTests 754, parserTest 391.
    snodeMessageTest
    streamNotifyTest
    streamTriggerTaskTest
    # These define/replace engine functions or global state and rely on their
    # own definitions being the only copy in the process; with the engine in a
    # DSO the executable no longer interposes the engine's internal calls.
    liteConsistencyTest
    taosdumpTests
    txnStateTest
    wlrRecorderTest
    xnodeRealFunctionsTest
    xnodeSimpleTest
    # Uses no engine code at all: a self-contained libcurl regression test
    # (eventfd double-close, CVE-2025-0665).  It links only ext_curl, dl and
    # gtest, so nothing provides the S3_* symbols that libtaosd_shared.so
    # deliberately leaves undefined, and with -z now the loader aborts before
    # main() ("symbol lookup error: libtaosd_shared.so: undefined symbol:
    # S3_initialize").  Add any further library-only test here as well.
    curlResolverFdTest
    # Tests that use the shared client (libtaos.so / libtaosnative.so): the
    # engine DSO also carries the client, and putting it first would interpose
    # the client API with a second copy of the client state.
    benchmarkTest
    blob_test
    clientBITest
    clientMonitorTest
    clientTest
    connectOptionsTest
    create_table
    driverIsolationTest
    get_db_name_test
    instanceTest
    jniSurfaceTest
    replay_test
    sessionTest
    stmt2Test
    taosdumpTest
    timezone_normalize_test
    timezone_test
    tmq_alter_tag
    tmq_base_on_test
    tmq_batch_alter_tag
    tmq_blob
    tmq_consumer_owner_check
    tmq_demo
    tmq_get_meta_json
    tmq_multi_thread_test
    tmq_offset
    tmq_offset_test
    tmq_poll_test
    tmq_sim
    tmq_spot_trading
    tmq_taosx_ci
    tmq_taosx_txn
    tmq_td32187
    tmq_td32471
    tmq_td32526
    tmq_td33798
    tmq_td35698
    tmq_td37265
    tmq_td37436
    tmq_td38404
    tmq_token
    tmq_ts5466
    tmq_ts5776
    tmq_ts6115
    tmq_ts7402
    tmq_ts7662
    tmq_vtable
    tmq_write_raw_test
    tmq_write_raw_with_blob_test
    tsz_test
    userOperTest
    varbinary_test
    write_raw_block_test
)

# Called from DEP_ext_gtest() for every googletest target.  The DSO is put on
# the link line as an option (before the objects) with --no-as-needed, so the
# engine symbols resolve from it and the static archives are never pulled.
# $<TARGET_FILE:taosd_shared> is resolved at generate time, which is why this
# does not require the target to exist yet.
function(td_test_link_shared_engine tgt)
    if(NOT BUILD_TEST_SHARED_ENGINE OR NOT TD_LINUX)
        return()
    endif()
    if(NOT TARGET ${tgt})
        return()
    endif()
    if(${tgt} IN_LIST TD_TEST_SHARED_ENGINE_SKIP)
        return()
    endif()
    # Executables get the DSO directly; helper libraries such as `sut` forward
    # it to every test that links them (those tests never call DEP_ext_gtest
    # themselves).
    get_target_property(_type ${tgt} TYPE)
    if(_type STREQUAL "EXECUTABLE")
        set(_scope PRIVATE)
    elseif(_type STREQUAL "STATIC_LIBRARY")
        set(_scope INTERFACE)
    else()
        return()
    endif()
    target_link_options(${tgt} ${_scope}
        "-Wl,--no-as-needed"
        "$<TARGET_FILE:taosd_shared>"
        "-Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib"
        # The DSO intentionally leaves the S3 entry points undefined (libs3.a
        # is not position independent and tests never call S3).  mold rejects
        # that by default, GNU ld does not.
        "-Wl,--allow-shlib-undefined"
    )
    # The DSO path above is just a file reference, so nothing orders it before
    # the test.  Remember the target here and add the dependency once
    # taosd_shared exists.
    set_property(GLOBAL APPEND PROPERTY TD_TEST_SHARED_ENGINE_LINKED ${tgt})
endfunction()

# Creates the shared engine.  Must run after all engine libraries exist.
function(td_add_test_shared_engine)
    if(NOT BUILD_TEST_SHARED_ENGINE OR NOT TD_LINUX)
        return()
    endif()

    # Link engine archives by file, not by target: passing the targets would
    # also propagate their interface dependencies, including the prebuilt
    # non-PIC libs3.a that cannot be placed in a shared object.  The external
    # archives they need are collected separately below.
    set(_engine_files "")
    set(_external_files "")
    foreach(_lib IN LISTS TD_TEST_SHARED_ENGINE_LIBS)
        list(APPEND _engine_files "$<TARGET_FILE:${_lib}>")
        get_target_property(_deps ${_lib} INTERFACE_LINK_LIBRARIES)
        if(_deps)
            foreach(_dep IN LISTS _deps)
                if(NOT TARGET ${_dep} AND _dep MATCHES "/")
                    list(APPEND _external_files "${_dep}")
                endif()
            endforeach()
        endif()
    endforeach()
    list(REMOVE_DUPLICATES _external_files)
    set(_external_keep "")
    foreach(_dep IN LISTS _external_files)
        # liblibs3.a is not position independent; tests do not use S3.
        if(NOT _dep MATCHES "libs3")
            list(APPEND _external_keep "${_dep}")
        endif()
    endforeach()

    list(REMOVE_ITEM _engine_files "$<TARGET_FILE:common>" "$<TARGET_FILE:taosk>")

    set(_anchor "${CMAKE_BINARY_DIR}/taosd_shared_anchor.c")
    if(NOT EXISTS "${_anchor}")
        file(WRITE "${_anchor}" "int tdTaosdSharedAnchor(void) { return 0; }\n")
    endif()

    add_library(taosd_shared SHARED "${_anchor}")
    # The engine archives below are linked by file path, so nothing orders their
    # build before this link and a parallel build fails with "No rule to make
    # target 'build/lib/libos.a'" before the script retries serially.
    add_dependencies(taosd_shared ${TD_TEST_SHARED_ENGINE_LIBS})
    target_link_libraries(taosd_shared PRIVATE
        -Wl,--whole-archive
        ${_engine_files}
        -Wl,--no-whole-archive
        # common is linked normally: its COS object references S3 globals that
        # only the prebuilt libs3.a defines.
        "$<TARGET_FILE:common>"
        "$<TARGET_FILE:taosk>"
        ${_external_keep}
        # libTSZ and ext_lzma2 both provide the zstd threading object.
        -Wl,--allow-multiple-definition
        # timer_create/timer_settime live in librt on glibc 2.28 (CI image).
        rt
    )
    set_target_properties(taosd_shared PROPERTIES
        OUTPUT_NAME taosd_shared
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib"
    )

    get_property(_linked GLOBAL PROPERTY TD_TEST_SHARED_ENGINE_LINKED)
    foreach(_tgt IN LISTS _linked)
        if(TARGET ${_tgt})
            add_dependencies(${_tgt} taosd_shared)
        endif()
    endforeach()

    message(STATUS "BUILD_TEST_SHARED_ENGINE=ON: unit tests link libtaosd_shared.so")
endfunction()
