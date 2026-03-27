IF(TD_TAOS_TOOLS)
    ADD_SUBDIRECTORY(taos-tools)
ENDIF()

add_subdirectory(shell)

IF(${TD_LINUX})
    add_subdirectory(rocks-reader)
ENDIF()

