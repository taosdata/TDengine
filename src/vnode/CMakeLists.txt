cmake_minimum_required(VERSION 2.8)

add_subdirectory(common)

add_subdirectory(tsdb)

enable_testing()

add_subdirectory(tests)