add_subdirectory(mgmt)

if(TD_LINUX)

find_path(HAVE_SYS_EPOLL_H sys/epoll.h)
if (HAVE_SYS_EPOLL_H)
	add_definitions("-DWITH_EPOLL")
endif()

aux_source_directory(mqtt/src MQTT_SRC)
add_executable(taosmqtt ${MQTT_SRC})

DEP_ext_cjson(taosmqtt)
DEP_ext_libuv(taosmqtt)

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
    "${TD_SOURCE_DIR}/include/client"
    "${TD_SOURCE_DIR}/include/dnode/bnode"
    "${TD_SOURCE_DIR}/contrib/libmqtt/inc"
    "${TD_SOURCE_DIR}/contrib/libmqtt/ttq/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/libs/tmqtt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mqtt/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/ttq/inc"
)

target_link_libraries(
    taosmqtt
    PRIVATE util common ${TAOS_NATIVE_LIB_STATIC} mqtt
)

# topic tool
add_subdirectory(tools)

endif()

# test
if(${BUILD_TEST})
    #add_subdirectory(test)
endif()
