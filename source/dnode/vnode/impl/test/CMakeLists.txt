# Vnode API test
add_executable(vnodeApiTests "")
target_sources(vnodeApiTests
    PRIVATE
    "vnodeApiTests.cpp"
)
target_link_libraries(vnodeApiTests vnode gtest gtest_main)