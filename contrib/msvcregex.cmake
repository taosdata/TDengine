cmake_minimum_required(VERSION 3.0)
project(msvcregex)

add_library(regex STATIC regex.c)
target_include_directories(regex PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(regex PROPERTIES PUBLIC_HEADER regex.h)
INSTALL(
    TARGETS regex
    PUBLIC_HEADER
)

