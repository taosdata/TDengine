aux_source_directory(src SHELL_SRC)

add_executable(shell ${SHELL_SRC})

IF(TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
    SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
    ADD_DEPENDENCIES(shell jemalloc)
ELSE()
    SET(LINK_JEMALLOC "")
ENDIF()

if(${FALLBACK})           # {
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
endif()                   # }

IF(TD_LINUX AND TD_ALPINE)
    SET(LINK_ARGP "/usr/lib/libargp.a")
ELSE()
    SET(LINK_ARGP "")
ENDIF()

if(TD_WINDOWS)
    target_link_libraries(shell PUBLIC ${TAOS_LIB_STATIC} ${LINK_WEBSOCKET})
else()
    target_link_libraries(shell PUBLIC ${TAOS_LIB} ${LINK_WEBSOCKET} ${LINK_JEMALLOC} ${LINK_ARGP})
endif()

if(NOT ${FALLBACK})           # {
    if(TD_WEBSOCKET) # freemine: what if -DWEBSOCKET=true is specified on command line?
        DEP_ext_taosws(shell)
    endif()
    if(${TD_WINDOWS})   # {
        target_link_libraries(shell PUBLIC Ntdll Bcrypt Secur32 Crypt32 Ncrypt)
        target_link_options(shell PUBLIC /NODEFAULTLIB:MSVCRT)
    endif()             # }
endif()                       # }
target_link_libraries(
    shell
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

    if(${FALLBACK})           # {
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
    else()                    # }{
        # freemine: to be depended by shell target
        DEP_ext_taosws(shell_ut)
        target_link_libraries(shell_ut PUBLIC ${TAOS_LIB} ${LINK_WEBSOCKET} ${LINK_JEMALLOC} ${LINK_ARGP})
        target_link_libraries(shell_ut PRIVATE os common transport geometry util)
    endif()                   # }

    # unit test
    IF(${BUILD_TEST})
        ADD_SUBDIRECTORY(test)
    ENDIF(${BUILD_TEST})    
ENDIF()
