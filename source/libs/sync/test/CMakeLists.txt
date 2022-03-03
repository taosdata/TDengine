add_executable(syncTest "")
add_executable(syncEnvTest "")
add_executable(syncPingTest "")
add_executable(syncEncodeTest "")
add_executable(syncIOTickQTest "")
add_executable(syncIOTickPingTest "")
add_executable(syncIOSendMsgTest "")
add_executable(syncIOSendMsgClientTest "")
add_executable(syncIOSendMsgServerTest "")
add_executable(syncRaftStoreTest "")


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
target_sources(syncIOTickQTest
    PRIVATE
    "syncIOTickQTest.cpp"
)
target_sources(syncIOTickPingTest
    PRIVATE
    "syncIOTickPingTest.cpp"
)
target_sources(syncIOSendMsgTest
    PRIVATE
    "syncIOSendMsgTest.cpp"
)
target_sources(syncIOSendMsgClientTest
    PRIVATE
    "syncIOSendMsgClientTest.cpp"
)
target_sources(syncIOSendMsgServerTest
    PRIVATE
    "syncIOSendMsgServerTest.cpp"
)
target_sources(syncRaftStoreTest
    PRIVATE
    "syncRaftStoreTest.cpp"
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
target_include_directories(syncIOTickQTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncIOTickPingTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncIOSendMsgTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncIOSendMsgClientTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncIOSendMsgServerTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncRaftStoreTest
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
target_link_libraries(syncIOTickQTest
    sync
    gtest_main
)
target_link_libraries(syncIOTickPingTest
    sync
    gtest_main
)
target_link_libraries(syncIOSendMsgTest
    sync
    gtest_main
)
target_link_libraries(syncIOSendMsgClientTest
    sync
    gtest_main
)
target_link_libraries(syncIOSendMsgServerTest
    sync
    gtest_main
)
target_link_libraries(syncRaftStoreTest
    sync
    gtest_main
)


enable_testing()
add_test(
    NAME sync_test
    COMMAND syncTest
)
