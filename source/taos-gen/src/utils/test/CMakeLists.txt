# Test LogUtils
add_executable(TestLogUtils
  TestLogUtils.cpp
)
target_link_libraries(TestLogUtils
  PRIVATE
    utils
)
add_test(NAME TestLogUtils COMMAND TestLogUtils)

# Test TimestampUtils
add_executable(TestTimestampUtils
  TestTimestampUtils.cpp
)
target_link_libraries(TestTimestampUtils
  PRIVATE
    utils
)
add_test(NAME TestTimestampUtils COMMAND TestTimestampUtils)

# Test ProcessUtils
add_executable(TestProcessUtils
  TestProcessUtils.cpp
)
target_link_libraries(TestProcessUtils
  PRIVATE
    utils
)
add_test(NAME TestProcessUtils COMMAND TestProcessUtils)

# Test SignalManager
add_executable(TestSignalManager
  TestSignalManager.cpp
)
target_link_libraries(TestSignalManager
  PRIVATE
    utils
)
add_test(NAME TestSignalManager COMMAND TestSignalManager)

# Test Barrier
add_executable(TestBarrier
  TestBarrier.cpp
)
target_link_libraries(TestBarrier
  PRIVATE
    utils
)
add_test(NAME TestBarrier COMMAND TestBarrier)

# Test Latch
add_executable(TestLatch
  TestLatch.cpp
)
target_link_libraries(TestLatch
  PRIVATE
    utils
)
add_test(NAME TestLatch COMMAND TestLatch)

# Test ScopedEnvVar
add_executable(TestScopedEnvVar
  TestScopedEnvVar.cpp
)
target_link_libraries(TestScopedEnvVar
  PRIVATE
    utils
)
add_test(NAME TestScopedEnvVar COMMAND TestScopedEnvVar)

# Test ConcurrentQueue
add_executable(TestConcurrentQueue
  TestConcurrentQueue.cpp
)
target_link_libraries(TestConcurrentQueue
  PRIVATE
    utils
)
add_test(NAME TestConcurrentQueue COMMAND TestConcurrentQueue)