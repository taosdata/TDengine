set(CMAKE_CXX_STANDARD 11)

add_executable(mgmtMnodeWorkerTest mmWorkerTest.cpp)
DEP_ext_gtest(mgmtMnodeWorkerTest)
target_include_directories(
    mgmtMnodeWorkerTest
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
)
target_link_libraries(mgmtMnodeWorkerTest PRIVATE mgmt_mnode mnode vnode)
target_link_options(
    mgmtMnodeWorkerTest
    PRIVATE
        -Wl,--wrap=rpcSendResponse
        -Wl,--wrap=rpcFreeCont
        -Wl,--wrap=tSingleWorkerCleanup
        -Wl,--wrap=qStreamGetReaderInfo
        -Wl,--wrap=syncIsReadyForRead
        -Wl,--wrap=initStorageAPI
        -Wl,--wrap=qDestroyTask
        -Wl,--wrap=qCreateStreamExecTaskInfo
        -Wl,--wrap=qResetTableScan
        -Wl,--wrap=qSetTaskId
        -Wl,--wrap=qUpdateOperatorParam
        -Wl,--wrap=qSetStreamGen
        -Wl,--wrap=setTaskScalarExtraInfo
        -Wl,--wrap=qExecTaskOpt
)

add_test(NAME mgmtMnodeWorkerTest COMMAND mgmtMnodeWorkerTest)
