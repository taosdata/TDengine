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

ADD_EXECUTABLE(vnodeTxnTest vnodeTxnTest.cpp)
DEP_ext_gtest(vnodeTxnTest)
TARGET_LINK_LIBRARIES(
         vnodeTxnTest
         PUBLIC os util common vnode mgmt_mnode vnode mgmt_mnode
)
IF(NOT TD_WINDOWS)
SET_TARGET_PROPERTIES(
         vnodeTxnTest PROPERTIES
         LINK_FLAGS "-Wl,--wrap=metaFetchEntryByUid -Wl,--wrap=metaFetchEntryFree -Wl,--wrap=metaDropTable2 -Wl,--wrap=metaRollbackAlterTable -Wl,--wrap=metaTxnIdxDelete -Wl,--wrap=metaScanTxnEntries -Wl,--wrap=taosArrayAddBatch"
)
ENDIF()

TARGET_INCLUDE_DIRECTORIES(
         bseTest
         PUBLIC "${TD_SOURCE_DIR}/include/common"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
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
