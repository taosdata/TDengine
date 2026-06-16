MESSAGE(STATUS "tq unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)

IF(NOT TD_WINDOWS)
    add_executable(tqTest tqTest.cpp)
    DEP_ext_gtest(tqTest)
    target_include_directories(tqTest
            PUBLIC
            "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )

    TARGET_LINK_LIBRARIES(
            tqTest
            PUBLIC os util common
    )
    # Handle circular dependencies by listing libraries multiple times
    # This works on all platforms (Linux, macOS, Windows)
    target_link_libraries(tqTest PUBLIC vnode mgmt_mnode vnode mgmt_mnode)

    enable_testing()

    add_test(
            NAME tq_test
            COMMAND tqTest
    )
ENDIF()


ADD_EXECUTABLE(bseTest bseTest.cpp)
DEP_ext_gtest(bseTest)
TARGET_LINK_LIBRARIES(
         bseTest
         PUBLIC os util common vnode
)

IF(TD_LINUX)
ADD_EXECUTABLE(vnodeTxnTest vnodeTxnTest.cpp)
DEP_ext_gtest(vnodeTxnTest)
TARGET_LINK_LIBRARIES(
         vnodeTxnTest
         PUBLIC os util common vnode mgmt_mnode
)
SET_TARGET_PROPERTIES(
         vnodeTxnTest PROPERTIES
         LINK_FLAGS "-Wl,--wrap=metaFetchEntryByUid -Wl,--wrap=metaFetchEntryFree -Wl,--wrap=metaDropTable2 -Wl,--wrap=metaRollbackAlterTable -Wl,--wrap=metaTxnIdxDelete -Wl,--wrap=metaScanTxnEntries -Wl,--wrap=metaScanTxnFinalEntries -Wl,--wrap=metaTxnFinalIdxDelete -Wl,--wrap=metaTxnFinalIdxUpsert -Wl,--wrap=taosArrayAddBatch -Wl,--wrap=metaMarkTableTxnStatus -Wl,--wrap=vnodeAsync -Wl,--wrap=metaRLock -Wl,--wrap=metaULock"
)

TARGET_INCLUDE_DIRECTORIES(
         vnodeTxnTest
         PUBLIC "${TD_SOURCE_DIR}/include/common"
         PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

add_test(
         NAME vnodeTxnTest
         COMMAND vnodeTxnTest
)

# Phase 1: metaSnapTxnTest disabled — snapshot sver-adjustment removed; re-enable in Phase 2
# ADD_EXECUTABLE(metaSnapTxnTest metaSnapTxnTest.cpp)
# DEP_ext_gtest(metaSnapTxnTest)
# TARGET_LINK_LIBRARIES(
#          metaSnapTxnTest
#          PUBLIC os util common vnode
# )
# SET_TARGET_PROPERTIES(
#          metaSnapTxnTest PROPERTIES
#          LINK_FLAGS "-Wl,--wrap=metaGetInfo -Wl,--wrap=metaScanTxnEntries -Wl,--wrap=tdbTbcOpen -Wl,--wrap=tdbTbcMoveTo -Wl,--wrap=tdbTbcClose -Wl,--wrap=metaRLock -Wl,--wrap=metaULock -Wl,--wrap=metaWLock -Wl,--wrap=metaCacheGet -Wl,--wrap=metaCacheUpsert"
# )
#
# TARGET_INCLUDE_DIRECTORIES(
#          metaSnapTxnTest
#          PUBLIC "${TD_SOURCE_DIR}/include/common"
#          PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
#          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
#          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )
#
# add_test(
#          NAME metaSnapTxnTest
#          COMMAND metaSnapTxnTest
# )
ENDIF()

TARGET_INCLUDE_DIRECTORIES(
         bseTest
         PUBLIC "${TD_SOURCE_DIR}/include/common"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
IF(NOT TD_WINDOWS)
    ADD_EXECUTABLE(tsdbCacheTest tsdbCacheTest.cpp)
    DEP_ext_gtest(tsdbCacheTest)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(tsdbCacheTest PRIVATE -fpermissive -Wno-error)
    endif()
    TARGET_LINK_LIBRARIES(
             tsdbCacheTest
             PUBLIC os util common vnode
    )

    TARGET_INCLUDE_DIRECTORIES(
             tsdbCacheTest
             PUBLIC "${TD_SOURCE_DIR}/include/common"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/tsdb"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
ENDIF()

# ---------------------------------------------------------------------------
# vnodeStreamVTable helpers unit test
# ---------------------------------------------------------------------------
IF(NOT TD_WINDOWS)
    add_executable(vnodeStreamVTableTest vnodeStreamVTableTest.cpp)
    DEP_ext_gtest(vnodeStreamVTableTest)
    target_include_directories(vnodeStreamVTableTest
        PUBLIC
        "${TD_SOURCE_DIR}/include/common"
        "${TD_SOURCE_DIR}/include/libs/new-stream"
        "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
    TARGET_LINK_LIBRARIES(vnodeStreamVTableTest PUBLIC os util common vnode mgmt_mnode vnode mgmt_mnode)
    add_test(NAME vnodeStreamVTableTest COMMAND vnodeStreamVTableTest)
ENDIF()

IF(NOT TD_WINDOWS)
    ADD_EXECUTABLE(tsdbFSEditTest tsdbFSEditTest.cpp)
    DEP_ext_gtest(tsdbFSEditTest)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(tsdbFSEditTest PRIVATE -fpermissive -Wno-error)
    endif()
    TARGET_LINK_LIBRARIES(
             tsdbFSEditTest
             PUBLIC os util common vnode
    )
    TARGET_INCLUDE_DIRECTORIES(
             tsdbFSEditTest
             PUBLIC "${TD_SOURCE_DIR}/include/common"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/tsdb"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
    enable_testing()
    add_test(
            NAME tsdb_fs_edit_test
            COMMAND tsdbFSEditTest
    )
ENDIF()

# ADD_EXECUTABLE(tsdbRepairTest tsdbRepairTest.cpp)
# DEP_ext_gtest(tsdbRepairTest)
# TARGET_LINK_LIBRARIES(
#          tsdbRepairTest
#          PUBLIC os util common vnode
# )

# TARGET_INCLUDE_DIRECTORIES(
#          tsdbRepairTest
#          PUBLIC "${TD_SOURCE_DIR}/include/common"
#          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
#          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/tsdb"
#          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )
