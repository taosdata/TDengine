add_executable(dmCorrelationTest dmCorrelationTest.c)
target_include_directories(dmCorrelationTest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../node_mgmt/inc")
target_link_libraries(dmCorrelationTest PRIVATE dnode)
add_test(NAME dmCorrelationTest COMMAND dmCorrelationTest)
