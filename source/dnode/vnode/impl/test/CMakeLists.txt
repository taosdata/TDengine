# Vnode API test
add_executable(vnodeApiTests "")
target_sources(vnodeApiTests
    PRIVATE
    "vnodeApiTests.cpp"
)
target_link_libraries(vnodeApiTests vnode gtest gtest_main)

add_test(
    NAME vnode_api_tests 
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/vnodeApiTests
    )