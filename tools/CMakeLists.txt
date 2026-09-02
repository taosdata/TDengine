IF(TD_TAOS_TOOLS)
    ADD_SUBDIRECTORY(taosBenchmark)
    add_subdirectory(taosdump)
ENDIF()

add_subdirectory(shell)

IF(TD_LINUX)
    add_subdirectory(rocks-reader)
ENDIF()

IF(BUILD_TOOLS)
    add_subdirectory(compressBench)
ENDIF()

# Storage diagnostic tools. Off by default: they are standalone diagnostics, not
# part of taosd, and are only needed when investigating a corruption report.
# Enable with -DBUILD_DIAG_TOOLS=ON.
IF(TD_LINUX AND BUILD_DIAG_TOOLS)
    add_subdirectory(rdb-selftest)
ENDIF()
