add_executable(blobTest "")
target_sources(blobTest
    PRIVATE
        blobTest.cpp
)
target_include_directories(blobTest
  PUBLIC
  ${TD_SOURCE_DIR}/include/libs/blob
  ${CMAKE_CURRENT_SOURCE_DIR}/../inc
)

target_link_libraries(blobTest
    PRIVATE
        blob
        gtest_main
)

enable_testing()
add_test(NAME blobTest COMMAND blobTest)
