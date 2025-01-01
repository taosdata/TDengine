add_executable(lemon lemon.c)

set(TD_CONTRIB_LEMON         "${CMAKE_BINARY_DIR}/build/bin/lemon" PARENT_SCOPE)
set(TD_CONTRIB_LEMON_SRC_DIR "${TD_CONTRIB_DIR}/lemon"             PARENT_SCOPE)

