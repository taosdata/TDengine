# CMakeLists.txt for taosk tool

# Minimum CMake version
cmake_minimum_required(VERSION 3.10)

# taosk is only supported on Linux platform (requires getopt.h)
if(NOT ${TD_LINUX})
    message(STATUS "taosk is only supported on Linux platform, skipping...")
    return()
endif()

# Project name
project(taosk C)

# Include directories
include_directories(
    "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    "${TD_COMMUNITY_DIR}/include/client"
    "${TD_COMMUNITY_DIR}/include/util"
    "${TD_COMMUNITY_DIR}/include/common"
    "${TD_COMMUNITY_DIR}/include/libs/crypt"
    "${TD_COMMUNITY_DIR}/include/libs/sm4"
)

# Library source files (taoskCrypt.c removed - now using community CBC encryption)
set(TAOSK_LIB_SRC
    src/taoskKey.c
    src/taoskFile.c
)

# Executable source files
set(TAOSK_EXEC_SRC
    src/taoskMain.c
)

# Create static library for linking with taosd
# This library provides taoskLoadEncryptKeys() and taoskSaveEncryptKeys()
add_library(taosk STATIC ${TAOSK_LIB_SRC})

# Create executable tool
add_executable(taosk_bin ${TAOSK_LIB_SRC} ${TAOSK_EXEC_SRC})

# Link libraries for the static library (OpenSSL removed - using community crypt)
target_link_libraries(taosk
    PUBLIC crypt
    PUBLIC os
    PUBLIC util
    PUBLIC common
)

# Link libraries for the executable
target_link_libraries(taosk_bin
    PRIVATE taosk
    PRIVATE os
    PRIVATE util
    PRIVATE common
    PRIVATE crypt
    PRIVATE transport
    PRIVATE grant
)

# Rename the binary to 'taosk' for installation
set_target_properties(taosk_bin PROPERTIES OUTPUT_NAME taosk)


