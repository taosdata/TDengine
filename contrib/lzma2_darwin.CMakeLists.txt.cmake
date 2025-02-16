cmake_minimum_required(VERSION 3.21)

project(xxhash)

add_library(fast-lzma2 STATIC xxhash.c)
set_target_properties(fast-lzma2
  PROPERTIES PUBLIC_HEADER xxhash.h
)
install(TARGETS fast-lzma2
  PUBLIC_HEADER
)


