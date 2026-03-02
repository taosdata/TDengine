cmake_minimum_required(VERSION 3.15)
project(test_cppstub CXX)

find_package(cppstub REQUIRED CONFIG)

add_executable(test_cppstub test_package.cpp)
target_link_libraries(test_cppstub cppstub::cppstub)
