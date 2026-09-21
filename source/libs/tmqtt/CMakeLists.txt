add_subdirectory(mgmt)

if(TD_LINUX OR TD_WINDOWS)

if(TD_LINUX)
    find_path(HAVE_SYS_EPOLL_H sys/epoll.h)
    if (HAVE_SYS_EPOLL_H)
        add_definitions("-DWITH_EPOLL")
    endif()
endif()

aux_source_directory(mqtt/src MQTT_SRC)
add_executable(taosmqtt ${MQTT_SRC})

DEP_ext_cjson(taosmqtt)
DEP_ext_libuv(taosmqtt)
DEP_ext_geos_INC(taosmqtt)
DEP_ext_pcre2_INC(taosmqtt)

target_compile_definitions(taosmqtt PRIVATE WITH_BROKER)

if(${TD_DARWIN})
    target_compile_options(taosmqtt PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    taosmqtt
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/"
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    "${TD_SOURCE_DIR}/include/libs/transport"
    "${TD_SOURCE_DIR}/include/libs/decimal"
    "${TD_SOURCE_DIR}/include/client"
    "${TD_SOURCE_DIR}/include/dnode/bnode"
    "${TD_SOURCE_DIR}/contrib/libmqtt/inc"
    "${TD_SOURCE_DIR}/contrib/libmqtt/ttq/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/libs/tmqtt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mqtt/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/ttq/inc"
)

# libtaos.so owns the public C API this broker calls (taos_connect, the TMQ
# consumer API) and dispatches to the driver it dlopen()s; libtaosnative.so is
# only needed for the engine internals used directly (functions *and* globals
# such as gMemPoolHandle, configDir, tsDataDir and the os/util logging and file
# helpers).  Naming the archives instead -- os/util/common or the client's
# ${TAOS_NATIVE_LIB_STATIC}, whose PUBLIC usage requirements drag in parser,
# planner, catalog, scheduler, function, qcom, ... -- links a second copy of the
# engine into this binary, including its global state.
#
# The Windows dispatcher is still fat and resolves every symbol at link time, so
# Windows keeps the previous static setup (mirrors tools/shell).
target_link_libraries(taosmqtt PRIVATE mqtt)
if(TD_WINDOWS)
    target_link_libraries(taosmqtt PRIVATE util common ${TAOS_NATIVE_LIB_STATIC} ws2_32)
else()
    # taosmqtt is only built on Linux and Windows, so this is the Linux case.
    target_link_libraries(taosmqtt PRIVATE ${TAOS_LIB} ${TAOS_NATIVE_LIB})
endif()

# topic tool (unix-only: links pthread)
if(TD_LINUX)
    add_subdirectory(tools)
endif()

endif()

# test
if(${BUILD_TEST})
    #add_subdirectory(test)
endif()
