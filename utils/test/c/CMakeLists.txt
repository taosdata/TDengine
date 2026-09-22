# These programs are internal test tools -- they are never shipped -- so they
# link the *shared* client libraries and no engine archive at all.  libtaos.so
# owns the public C API (taos_*, tmq_*), libtaosnative.so provides the engine
# symbols they call directly, and both are needed.
#
# Two things must never appear on their link line:
#
#   * the engine archives (util/common/os).  The linker prefers an archive
#     member over a shared library, so listing them makes each tool embed its
#     own copy of that code -- and of the global state inside it.  The
#     executable then becomes the provider for libtaosnative.so, which
#     initialises the executable's copy and leaves the driver's uninitialised,
#     so taosInitCfg fails and taos_connect() returns NULL (TMQ tests then sit
#     at "row: 0" until they time out).
#   * ${TAOS_NATIVE_LIB_STATIC}.  Its PUBLIC engine dependencies drag the whole
#     static stack into every executable: each tool grows to ~79 MB and the
#     release package by ~1.9 GB.
#
# Engine symbols these tools import are exported through the libtaosnative.so
# whitelist (source/client/taosnative.ver); regenerate it with
# cmake/gen_export_ver.py after a tool starts using a new one, otherwise the
# link fails or, worse, some archive satisfies it silently.
#
# -lm: dropping the engine archives also drops their usage requirements, and a
# few tools (tmq_sim, ...) call ceil()/floor() directly.
set(TD_TEST_C_LIBS ${TAOS_LIB} ${TAOS_NATIVE_LIB} m)

add_executable(tmq_demo tmqDemo.c)
add_dependencies(tmq_demo ${TAOS_NATIVE_LIB})
add_executable(tmq_sim tmqSim.c)
add_executable(create_table createTable.c)
add_executable(tmq_taosx_ci tmq_taosx_ci.c)
add_executable(tmq_ts5466 tmq_ts5466.c)
add_executable(tmq_vtable tmq_vtable.c)
add_executable(tmq_td38404 tmq_td38404.c)
add_executable(tmq_td37436 tmq_td37436.c)
add_executable(tmq_td32526 tmq_td32526.c)
add_executable(tmq_td32187 tmq_td32187.c)
add_executable(tmq_ts5776 tmq_ts5776.c)
add_executable(tmq_ts6115 tmq_ts6115.c)
add_executable(tmq_ts7662 tmq_ts7662.c)
add_executable(tmq_td32471 tmq_td32471.c)
add_executable(tmq_td35698 tmq_td35698.c)
add_executable(tmq_spot_trading tmq_spot_trading.c)
add_executable(tmq_get_meta_json tmq_get_meta_json.c)
add_executable(tmq_td33798 tmq_td33798.c)
add_executable(tmq_td37265 tmq_td37265.c)
add_executable(tmq_ts7402 tmq_ts7402.c)
add_executable(tmq_token tmq_token.c)
add_executable(tmq_batch_alter_tag tmq_batch_alter_tag.c)
add_executable(tmq_alter_tag tmq_alter_tag.c)
add_executable(tmq_poll_test tmq_poll_test.c)
add_executable(tmq_write_raw_test tmq_write_raw_test.c)
add_executable(tmq_write_raw_with_blob_test tmq_write_raw_with_blob_test.c)
add_executable(write_raw_block_test write_raw_block_test.c)
add_executable(sml_test sml_test.c)
add_executable(get_db_name_test get_db_name_test.c)
add_executable(tmq_offset tmqOffset.c)
add_executable(tmq_multi_thread_test tmq_multi_thread_test.c)
add_executable(tmq_offset_test tmq_offset_test.c)
add_executable(varbinary_test varbinary_test.c)
add_executable(blob_test blob_test.c)
add_executable(replay_test replay_test.c)
add_executable(timezone_test timezone_test.c)
add_executable(timezone_normalize_test timezone_normalize_test.c)
add_executable(tmq_blob tmq_blob.c)
add_executable(tmq_base_on_test tmq_base_on_test.c)
add_executable(tmq_taosx_txn tmq_taosx_txn.c)

if(TD_LINUX)
    add_executable(tsz_test tsz_test.c)
endif()

target_link_libraries(
    tmq_offset
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_multi_thread_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    create_table
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_demo
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_sim
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_token
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_ts5466
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_vtable
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_alter_tag
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_ts7662
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_td38404
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_td32187
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_td35698
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_spot_trading
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_get_meta_json
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_td32471
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_td37436
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_td37265
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_ts7402
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_td33798
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_ts6115
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_poll_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_td32526
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_batch_alter_tag
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_ts5776
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_taosx_ci
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_offset_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    replay_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    write_raw_block_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_write_raw_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_base_on_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_write_raw_with_blob_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    sml_test
    PUBLIC ${TD_TEST_C_LIBS}
    # sml_test decodes WKT and calls geosFreeBuffer() on the result.  GEOS is
    # pure computation, so the archive cannot split engine state; it stays until
    # the symbol is proven to resolve from libtaosnative.so.
    PUBLIC geometry
)

target_link_libraries(
    get_db_name_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    varbinary_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    blob_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    timezone_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    timezone_normalize_test
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_blob
    PUBLIC ${TD_TEST_C_LIBS}
)

target_link_libraries(
    tmq_taosx_txn
    PUBLIC ${TD_TEST_C_LIBS}
)

if(${TD_LINUX})
    # tsz_test drives the SZ codec's internals directly (cost_start/cost_end,
    # createHuffmanTree, SZ_ReleaseHuffman).  Those live in contrib/TSZ, which
    # the driver links with --exclude-libs,libTSZ.a -- so they are local in
    # libtaosnative.so and cannot be exported no matter what the whitelist says.
    # They used to arrive transitively through util's PUBLIC TSZ link, which is
    # gone now.  SZ is pure codec code with no engine global state, so embedding
    # it here cannot split anything.
    target_link_libraries(
        tsz_test
        PUBLIC ${TD_TEST_C_LIBS}
        PUBLIC TSZ
    )
endif()

# These three build and parse JSON payloads themselves (cJSON_*), and cJSON is
# linked into the driver with --exclude-libs,libcjson.a -- so its symbols are
# local in libtaosnative.so and can never be exported.  common used to bring the
# archive in transitively; DEP_ext_cjson is the same call common, taosmqtt and
# dmodule make.
DEP_ext_cjson(tmq_base_on_test)
DEP_ext_cjson(tmq_spot_trading)
DEP_ext_cjson(tmq_taosx_ci)

