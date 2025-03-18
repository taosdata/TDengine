aux_source_directory(src SHELL_SRC)

add_executable(shell ${SHELL_SRC})

IF(TD_LINUX AND TD_WEBSOCKET)
    ADD_DEFINITIONS(-DWEBSOCKET -I${CMAKE_BINARY_DIR}/build/include -ltaosws)
    SET(LINK_WEBSOCKET "-L${CMAKE_BINARY_DIR}/build/lib -ltaosws")
    ADD_DEPENDENCIES(shell taosws-rs)
ELSEIF(TD_DARWIN AND TD_WEBSOCKET)
    ADD_DEFINITIONS(-DWEBSOCKET -I${CMAKE_BINARY_DIR}/build/include)
    SET(LINK_WEBSOCKET "${CMAKE_BINARY_DIR}/build/lib/libtaosws.dylib")
    ADD_DEPENDENCIES(shell taosws-rs)
ELSEIF(TD_WINDOWS AND TD_WEBSOCKET)
    ADD_DEFINITIONS(-DWEBSOCKET -I${CMAKE_BINARY_DIR}/build/include)
    SET(LINK_WEBSOCKET "${CMAKE_BINARY_DIR}/build/lib/taosws.lib")
    ADD_DEPENDENCIES(shell taosws-rs)
ELSE()
    SET(LINK_WEBSOCKET "")
ENDIF()

IF(TD_LINUX AND TD_ALPINE)
    SET(LINK_ARGP "/usr/lib/libargp.a")
ELSE()
    SET(LINK_ARGP "")
ENDIF()

if(TD_WINDOWS)
    target_link_libraries(shell PUBLIC ${TAOS_LIB_STATIC})
else()
    target_link_libraries(shell PUBLIC ${TAOS_LIB} ${LINK_ARGP})
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
    add_library(shell_ut STATIC ${SHELL_SRC})

    IF(TD_WEBSOCKET)
        ADD_DEPENDENCIES(shell_ut taosws-rs)
    ENDIF()

    target_link_libraries(shell_ut PUBLIC ${TAOS_LIB} ${LINK_WEBSOCKET} ${LINK_JEMALLOC} ${LINK_ARGP})
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
