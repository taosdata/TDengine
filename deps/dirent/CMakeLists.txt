cmake_minimum_required (VERSION 2.8.11)
project (dirent LANGUAGES C CXX)

# Initialize C and C++ compilers
enable_language (C CXX)

# Compile in debug mode by default
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Debug
        CACHE STRING
        "Type of build: None Debug Release RelWithDebInfo MinSizeRel."
        FORCE
    )
endif (NOT CMAKE_BUILD_TYPE)

# Use the include directory only on Windows
if (WIN32)
    include_directories (${CMAKE_SOURCE_DIR}/include)
endif (WIN32)

# Install dirent.h
if (WIN32)
    install (FILES include/dirent.h DESTINATION include)
endif (WIN32)

# Add distclean target
add_custom_target (distclean
    COMMAND ${CMAKE_BUILD_TOOL} clean
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_SOURCE_DIR}/distclean.cmake
)

# Build example programs
add_executable (find examples/find.c)
add_executable (ls examples/ls.c)
add_executable (locate examples/locate.c)
add_executable (updatedb examples/updatedb.c)
add_executable (scandir examples/scandir.c)
add_executable (cat examples/cat.c)

# Build test programs
include (CTest)
add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -C ${CMAKE_CFG_INTDIR})
function (add_test_executable TEST_NAME)
    add_executable (${TEST_NAME} EXCLUDE_FROM_ALL ${ARGN})
    add_test (NAME ${TEST_NAME} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} COMMAND $<TARGET_FILE:${TEST_NAME}>)
    add_dependencies (check ${TEST_NAME})
endfunction (add_test_executable)

add_test_executable (t-compile tests/t-compile.c)
add_test_executable (t-dirent tests/t-dirent.c)
add_test_executable (t-scandir tests/t-scandir.c)
add_test_executable (t-unicode tests/t-unicode.c)
add_test_executable (t-cplusplus tests/t-cplusplus.cpp)
