add_executable(snodeMessageTest snodeMessageTest.cpp)
DEP_ext_gtest(snodeMessageTest)
DEP_ext_cppstub(snodeMessageTest)
target_include_directories(snodeMessageTest PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    "${TD_SOURCE_DIR}/source/libs/new-stream/inc"
)
target_link_libraries(snodeMessageTest PRIVATE snode os common new-stream executor transport qcom nodes function index)
target_link_options(snodeMessageTest PRIVATE
    -Wl,--wrap=rpcSendResponse
    -Wl,--wrap=tmsgSendRsp
    -Wl,--wrap=stRunnerTaskExecute
    -Wl,--wrap=stRunnerTaskDropTable
)
add_test(NAME snodeMessageTest COMMAND snodeMessageTest)
