add_executable(syncTest "")
add_executable(syncEnvTest "")
add_executable(syncPingTest "")
add_executable(syncEncodeTest "")
add_executable(syncIOTickQ "")
add_executable(syncIOTickPing "")


target_sources(syncTest
    PRIVATE
    "syncTest.cpp"
)
target_sources(syncEnvTest
    PRIVATE
    "syncEnvTest.cpp"
)
target_sources(syncPingTest
    PRIVATE
    "syncPingTest.cpp"
)
target_sources(syncEncodeTest
    PRIVATE
    "syncEncodeTest.cpp"
)
target_sources(syncIOTickQ
    PRIVATE
    "syncIOTickQ.cpp"
)
target_sources(syncIOTickPing
    PRIVATE
    "syncIOTickPing.cpp"
)


target_include_directories(syncTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncEnvTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncPingTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncEncodeTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncIOTickQ
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncIOTickPing
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)


target_link_libraries(syncTest
    sync
    gtest_main
)
target_link_libraries(syncEnvTest
    sync
    gtest_main
)
target_link_libraries(syncPingTest
    sync
    gtest_main
)
target_link_libraries(syncEncodeTest
    sync
    gtest_main
)
target_link_libraries(syncIOTickQ
    sync
    gtest_main
)
target_link_libraries(syncIOTickPing
    sync
    gtest_main
)


enable_testing()
add_test(
    NAME sync_test
    COMMAND syncTest
)
