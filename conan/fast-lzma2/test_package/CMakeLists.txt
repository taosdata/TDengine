cmake_minimum_required(VERSION 3.15)
project(test_fast_lzma2 C)

find_package(fast-lzma2 REQUIRED CONFIG)

add_executable(test_fast_lzma2 test_package.c)
target_link_libraries(test_fast_lzma2 fast-lzma2::fast-lzma2)
