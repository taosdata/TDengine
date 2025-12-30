add_executable(xnodeEncodeDecodeTest "xnodeEncodeDecodeTest.cpp")
target_include_directories(
  xnodeEncodeDecodeTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
)
target_link_libraries(
    xnodeEncodeDecodeTest
    PUBLIC sut
    PUBLIC sdb
    PUBLIC gtest_main
)

add_test(
    NAME xnodeEncodeDecodeTest
    COMMAND xnodeEncodeDecodeTest
)

# Simplified standalone test
add_executable(xnodeSimpleTest "xnodeSimpleTest.cpp")
target_include_directories(
  xnodeSimpleTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
)

target_link_libraries(
    xnodeSimpleTest
    PUBLIC gtest
    PUBLIC gtest_main
    PUBLIC pthread
)

add_test(
    NAME xnodeSimpleTest
    COMMAND xnodeSimpleTest
)

# Memory management tests
add_executable(xnodeMemoryTest "xnodeMemoryTest.cpp")
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
    PUBLIC gtest
    PUBLIC gtest_main
    PUBLIC pthread
    mnode
    xnode
    mgmt_xnode 
)

add_test(
    NAME xnodeMemoryTest
    COMMAND xnodeMemoryTest
)

# Test real XNode utility functions (completely independent, no TDengine headers)
add_executable(xnodeRealFunctionsTest "xnodeRealFunctionsTest.cpp")
target_link_libraries(xnodeRealFunctionsTest
    PUBLIC gtest
    PUBLIC gtest_main
    PUBLIC pthread
)

add_test(
    NAME xnodeRealFunctionsTest
    COMMAND xnodeRealFunctionsTest
)

# SDB operations tests (encode/decode, insert/update/delete)
# Uses mock implementations that mirror production code logic
add_executable(xnodeSdbTest "xnodeSdbTest.cpp")
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
    PUBLIC gtest
    PUBLIC gtest_main
    PUBLIC pthread
    mnode
)

add_test(
    NAME xnodeSdbTest
    COMMAND xnodeSdbTest
)

# Production code tests - uses REAL encode/decode functions from mndXnode.c
add_executable(xnodeProductionTest "xnodeProductionTest.cpp")
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
    PUBLIC gtest
    PUBLIC gtest_main
    PUBLIC pthread
    mnode
    xnode
    mgmt_xnode 
)

add_test(
    NAME xnodeProductionTest
    COMMAND xnodeProductionTest
)
