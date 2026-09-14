set(CMAKE_CXX_STANDARD 11)

add_executable(txnodeEnvTest txnodeEnvTest.cpp)
DEP_ext_gtest(txnodeEnvTest)
target_include_directories(txnodeEnvTest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc")
target_link_libraries(txnodeEnvTest PRIVATE txnode xnode os util common nodes transport)
add_test(NAME txnodeEnvTest COMMAND txnodeEnvTest)
