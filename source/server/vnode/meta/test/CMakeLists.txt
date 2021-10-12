add_executable(metaTest "")
target_sources(metaTest
    PRIVATE
    "../src/meta.c"
    "metaTests.cpp"
)
target_include_directories(metaTest 
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/vnode/meta"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_link_libraries(metaTest
    os
    util
    common
    rocksdb
    gtest_main
)