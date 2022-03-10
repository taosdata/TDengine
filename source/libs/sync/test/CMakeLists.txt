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
add_executable(syncEnqTest "")
add_executable(syncIndexTest "")
add_executable(syncInitTest "")
add_executable(syncUtilTest "")
add_executable(syncVotesGrantedTest "")
add_executable(syncVotesRespondTest "")
add_executable(syncIndexMgrTest "")
add_executable(syncLogStoreTest "")


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
target_sources(syncEnqTest
    PRIVATE
    "syncEnqTest.cpp"
)
target_sources(syncIndexTest
    PRIVATE
    "syncIndexTest.cpp"
)
target_sources(syncInitTest
    PRIVATE
    "syncInitTest.cpp"
)
target_sources(syncUtilTest
    PRIVATE
    "syncUtilTest.cpp"
)
target_sources(syncVotesGrantedTest
    PRIVATE
    "syncVotesGrantedTest.cpp"
)
target_sources(syncVotesRespondTest
    PRIVATE
    "syncVotesRespondTest.cpp"
)
target_sources(syncIndexMgrTest
    PRIVATE
    "syncIndexMgrTest.cpp"
)
target_sources(syncLogStoreTest
    PRIVATE
    "syncLogStoreTest.cpp"
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
target_include_directories(syncEnqTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncIndexTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncInitTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncUtilTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncVotesGrantedTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncVotesRespondTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncIndexMgrTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_include_directories(syncLogStoreTest
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
target_link_libraries(syncEnqTest
    sync
    gtest_main
)
target_link_libraries(syncIndexTest
    sync
    gtest_main
)
target_link_libraries(syncInitTest
    sync
    gtest_main
)
target_link_libraries(syncUtilTest
    sync
    gtest_main
)
target_link_libraries(syncVotesGrantedTest
    sync
    gtest_main
)
target_link_libraries(syncVotesRespondTest
    sync
    gtest_main
)
target_link_libraries(syncIndexMgrTest
    sync
    gtest_main
)
target_link_libraries(syncLogStoreTest
    sync
    gtest_main
)


enable_testing()
add_test(
    NAME sync_test
    COMMAND syncTest
)
