# taosdump unit tests

# bckName: percent-encoding of backup path components (db/stb/tb names).
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../inc)

add_executable(taosdumpTests taosdumpTests.cpp ../src/core/bckName.c)
DEP_ext_gtest(taosdumpTests)
add_test(
    NAME taosdumpTests
    COMMAND taosdumpTests
)
