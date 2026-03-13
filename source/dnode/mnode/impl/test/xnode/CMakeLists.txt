# Common include directories for tests that use mnode internal headers
set(XNODE_MNODE_INCLUDES
  "${TD_SOURCE_DIR}/include/dnode/mnode"
  "${TD_SOURCE_DIR}/include/dnode/mgmt"
  "${TD_SOURCE_DIR}/include/dnode/xnode"
  "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
  "${TD_SOURCE_DIR}/source/dnode/mnode/sdb/inc"
  "${TD_SOURCE_DIR}/include/libs/scheduler"
  "${TD_SOURCE_DIR}/include/libs/catalog"
  "${TD_SOURCE_DIR}/include/libs/planner"
  "${TD_SOURCE_DIR}/include/libs/qcom"
  "${TD_SOURCE_DIR}/include/libs/nodes"
  "${TD_SOURCE_DIR}/include/libs/transport"
  "${TD_SOURCE_DIR}/include/libs/sync"
  "${TD_SOURCE_DIR}/include/libs/monitor"
  "${TD_SOURCE_DIR}/include/libs/audit"
  "${TD_SOURCE_DIR}/include/libs/wal"
  "${TD_SOURCE_DIR}/include/libs/index"
  "${TD_SOURCE_DIR}/include/libs/function"
  "${TD_SOURCE_DIR}/include/libs/scalar"
  "${TD_SOURCE_DIR}/include/libs/executor"
  "${TD_SOURCE_DIR}/include/libs/new-stream"
  "${TD_SOURCE_DIR}/include/libs/qworker"
  "${TD_SOURCE_DIR}/include/libs/parser"
  "${TD_SOURCE_DIR}/include/libs/command"
  "${TD_SOURCE_DIR}/include/libs/monitorfw"
  "${TD_SOURCE_DIR}/include/libs/crypt"
  "${TD_SOURCE_DIR}/include/libs/tfs"
  "${TD_SOURCE_DIR}/include/libs/sm4"
  "${TD_SOURCE_DIR}/include/libs/geometry"
  "${TD_SOURCE_DIR}/include/libs/decimal"
  "${TD_SOURCE_DIR}/include/libs/tdb"
  "${TD_SOURCE_DIR}/include/common"
  "${TD_SOURCE_DIR}/include/util"
  "${TD_SOURCE_DIR}/include/os"
)

add_executable(xnodeEncodeDecodeTest "xnodeEncodeDecodeTest.cpp")
DEP_ext_gtest(xnodeEncodeDecodeTest)
target_include_directories(
  xnodeEncodeDecodeTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
)
target_link_libraries(
  xnodeEncodeDecodeTest
  PUBLIC sut
  sdb
)

add_test(
  NAME xnodeEncodeDecodeTest
  COMMAND xnodeEncodeDecodeTest
)

# Simplified standalone test
add_executable(xnodeSimpleTest "xnodeSimpleTest.cpp")
DEP_ext_gtest(xnodeSimpleTest)
target_include_directories(
  xnodeSimpleTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/common"
)

target_link_libraries(
  xnodeSimpleTest
  PUBLIC
)

add_test(
  NAME xnodeSimpleTest
  COMMAND xnodeSimpleTest
)

# Memory management tests
add_executable(xnodeMemoryTest "xnodeMemoryTest.cpp")
DEP_ext_gtest(xnodeMemoryTest)
target_include_directories(
  xnodeMemoryTest
  PUBLIC
  ${XNODE_MNODE_INCLUDES}
)
target_link_libraries(
  xnodeMemoryTest
  PUBLIC
  mnode xnode mgmt_xnode os util
)

add_test(
  NAME xnodeMemoryTest
  COMMAND xnodeMemoryTest
)

# Test real XNode utility functions (completely independent, no TDengine headers)
add_executable(xnodeRealFunctionsTest "xnodeRealFunctionsTest.cpp")
DEP_ext_gtest(xnodeRealFunctionsTest)
target_link_libraries(xnodeRealFunctionsTest
  PUBLIC
)

add_test(
  NAME xnodeRealFunctionsTest
  COMMAND xnodeRealFunctionsTest
)

# SDB operations tests (encode/decode, insert/update/delete)
# Uses mock implementations that mirror production code logic
add_executable(xnodeSdbTest "xnodeSdbTest.cpp")
DEP_ext_gtest(xnodeSdbTest)
target_include_directories(
  xnodeSdbTest
  PUBLIC
  ${XNODE_MNODE_INCLUDES}
)
target_link_libraries(xnodeSdbTest
  PUBLIC
  mnode xnode txnode os util
)

add_test(
  NAME xnodeSdbTest
  COMMAND xnodeSdbTest
)

# Production code tests - uses REAL encode/decode functions from mndXnode.c
add_executable(xnodeProductionTest "xnodeProductionTest.cpp")
DEP_ext_gtest(xnodeProductionTest)
target_include_directories(
  xnodeProductionTest
  PUBLIC
  ${XNODE_MNODE_INCLUDES}
)
target_link_libraries(xnodeProductionTest
  PUBLIC
  mnode
  xnode
  mgmt_xnode
)

add_test(
  NAME xnodeProductionTest
  COMMAND xnodeProductionTest
)

# Agent code tests - uses REAL agent functions from mndXnode.c
add_executable(xnodeAgentTest "xnodeAgentTest.cpp")
DEP_ext_gtest(xnodeAgentTest)
target_include_directories(
  xnodeAgentTest
  PUBLIC
  ${XNODE_MNODE_INCLUDES}
)
target_link_libraries(xnodeAgentTest
  PUBLIC
  mnode
  xnode
  mgmt_xnode
)

add_test(
  NAME xnodeAgentTest
  COMMAND xnodeAgentTest
)
