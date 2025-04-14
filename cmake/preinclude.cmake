if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24")
  cmake_policy(SET CMP0135 NEW)
endif()

include(ExternalProject)
include(CheckIncludeFile)
include(CheckLibraryExists)
include(CheckSymbolExists)