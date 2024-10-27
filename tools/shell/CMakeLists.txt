aux_source_directory(src SHELL_SRC)

add_executable(shell ${SHELL_SRC})

# include
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/inc)

LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellMain.c)
LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellAuto.c)
LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellTire.c)
LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellArguments.c)
LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellCommand.c)
LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellEngine.c)
LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellUtil.c)
LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellNettest.c)
LIST(APPEND SHELL_UT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/shellWebsocket.c)

# shell_ut library
add_library(shell_ut STATIC ${SHELL_UT_SRC})

IF(TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
    SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
    ADD_DEPENDENCIES(shell jemalloc)
ELSE()
    SET(LINK_JEMALLOC "")
ENDIF()

IF(TD_LINUX AND TD_WEBSOCKET)
    ADD_DEFINITIONS(-DWEBSOCKET -I${CMAKE_BINARY_DIR}/build/include -ltaosws)
    SET(LINK_WEBSOCKET "-L${CMAKE_BINARY_DIR}/build/lib -ltaosws")
    ADD_DEPENDENCIES(shell taosws-rs)
    ADD_DEPENDENCIES(shell_ut taosws-rs)
ELSEIF(TD_DARWIN AND TD_WEBSOCKET)
    ADD_DEFINITIONS(-DWEBSOCKET -I${CMAKE_BINARY_DIR}/build/include)
    SET(LINK_WEBSOCKET "${CMAKE_BINARY_DIR}/build/lib/libtaosws.dylib")
    ADD_DEPENDENCIES(shell taosws-rs)
    ADD_DEPENDENCIES(shell_ut taosws-rs)
ELSEIF(TD_WINDOWS AND TD_WEBSOCKET)
    ADD_DEFINITIONS(-DWEBSOCKET -I${CMAKE_BINARY_DIR}/build/include)
    SET(LINK_WEBSOCKET "${CMAKE_BINARY_DIR}/build/lib/taosws.lib")
    ADD_DEPENDENCIES(shell taosws-rs)
    ADD_DEPENDENCIES(shell_ut taosws-rs)
ELSE()
    SET(LINK_WEBSOCKET "")
ENDIF()

IF(TD_LINUX AND TD_ALPINE)
    SET(LINK_ARGP "/usr/lib/libargp.a")
ELSE()
    SET(LINK_ARGP "")
ENDIF()

if(TD_WINDOWS)
    target_link_libraries(shell PUBLIC taos_static ${LINK_WEBSOCKET})
else()
    target_link_libraries(shell PUBLIC taos ${LINK_WEBSOCKET} ${LINK_JEMALLOC} ${LINK_ARGP})
endif()

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

# link public
if(TD_WINDOWS)
    target_link_libraries(shell_ut PUBLIC taos_static ${LINK_WEBSOCKET})
else()
    target_link_libraries(shell_ut PUBLIC taos ${LINK_WEBSOCKET} ${LINK_JEMALLOC} ${LINK_ARGP})
endif()

# link private
target_link_libraries(
    shell_ut
    PRIVATE os common transport geometry util
)

# util depends
target_link_directories(
    shell_ut
    PUBLIC "${TD_SOURCE_DIR}/contrib/lzma2"
    PUBLIC "${TD_SOURCE_DIR}/contrib/pcre2"
)

# unit test
if(${BUILD_TEST})
  ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})