# vnodeMemAllocatorTest
add_executable(VMATest "")
target_sources(VMATest
    PRIVATE
    "../src/vnodeMemAllocator.c"
    "vnodeMemAllocatorTest.cpp"
)
target_include_directories(VMATest PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc")
target_link_libraries(VMATest os gtest_main)