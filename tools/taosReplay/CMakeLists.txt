# taosreplay -- development tool, deliberately NOT installed (FS 4.10, DS 10.1).
# There is no install() rule here on purpose: the tool reads recordings that contain
# unmasked SQL, and shipping it in the product package would invite running it
# against production.
aux_source_directory(src REPLAY_SRC)

add_executable(taosreplay ${REPLAY_SRC})

target_include_directories(taosreplay
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/wlr/inc"
)

# Public taos.h only, plus the shared format library. No client internals: that is
# what keeps the recording format the actual contract between the two (DS 7.2).
# Public taos_* entry points are exported by the dispatcher, while the native
# driver remains an explicit dependency for the dispatcher's engine symbols.
target_link_libraries(taosreplay
    PRIVATE ${TAOS_LIB}
    PRIVATE ${TAOS_NATIVE_LIB}
    PRIVATE wlr
    PRIVATE os
    PRIVATE util
)

if(BUILD_TEST)
    add_executable(replayLaneTest test/replayLaneTest.cpp src/replayLane.c src/replayInput.c)
    DEP_ext_gtest(replayLaneTest)
    target_include_directories(replayLaneTest PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/inc"
        "${TD_SOURCE_DIR}/include/client"
        "${TD_SOURCE_DIR}/source/libs/wlr/inc"
    )
    target_link_libraries(replayLaneTest PRIVATE ${TAOS_LIB} ${TAOS_NATIVE_LIB} wlr os util)
    add_test(NAME replay_lane_test COMMAND replayLaneTest)
    set_tests_properties(replay_lane_test PROPERTIES TIMEOUT 30)
endif()
