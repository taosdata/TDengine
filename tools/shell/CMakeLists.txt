aux_source_directory(src SHELL_SRC)

add_executable(shell ${SHELL_SRC} ../src/pub.c)

IF(TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
    SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
    ADD_DEPENDENCIES(shell jemalloc)
ELSE()
    SET(LINK_JEMALLOC "")
ENDIF()

IF(TD_LINUX AND TD_ALPINE)
    SET(LINK_ARGP "/usr/lib/libargp.a")
ELSE()
    SET(LINK_ARGP "")
ENDIF()

if(TD_WINDOWS)
    target_link_libraries(shell PUBLIC ${TAOS_LIB})
else()
    target_link_libraries(shell PUBLIC ${TAOS_LIB} ${LINK_JEMALLOC} ${LINK_ARGP})
endif()

target_link_libraries(
    shell
    PUBLIC ${LINK_WEBSOCKET}
    PRIVATE os common transport geometry util
)

target_include_directories(
    shell
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(shell PROPERTIES OUTPUT_NAME taos)

#
# generator library shell_ut for uint test
#
IF(TD_LINUX)
    # include
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/inc)


    # shell_ut library
    add_library(shell_ut STATIC ${SHELL_SRC} ../src/pub.c)

    target_link_libraries(shell_ut PUBLIC ${TAOS_LIB} ${LINK_JEMALLOC} ${LINK_ARGP})
    target_link_libraries(shell_ut PRIVATE os common transport geometry util)

    # util depends
    target_link_directories(
        shell_ut
        PUBLIC "${TD_SOURCE_DIR}/contrib/lzma2"
        PUBLIC "${TD_SOURCE_DIR}/contrib/pcre2"
    )

    # unit test
    IF(${BUILD_TEST})
        ADD_SUBDIRECTORY(test)
    ENDIF(${BUILD_TEST})
ENDIF()

#
# collect --version information
#
MESSAGE("collect --version show info:")
# version
IF (DEFINED TD_VER_NUMBER)
    ADD_DEFINITIONS(-DTD_VER_NUMBER="${TD_VER_NUMBER}")
    MESSAGE(STATUS "taos version:${TD_VER_NUMBER}")
ELSE ()    
    # abort build
    MESSAGE(FATAL_ERROR "build taos not found TD_VER_NUMBER define.")
ENDIF ()