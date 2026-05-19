aux_source_directory(src MQTT_SRC)
aux_source_directory(ttq/src MQTT_TTQ_SRC)

if(TD_LINUX)

add_library(mqtt STATIC ${MQTT_SRC} ${MQTT_TTQ_SRC})

find_path(HAVE_SYS_EPOLL_H sys/epoll.h)
if (HAVE_SYS_EPOLL_H)
	add_definitions("-DWITH_EPOLL")
endif()

target_compile_definitions(mqtt PRIVATE WITH_BROKER)

if(${TD_DARWIN})
    target_compile_options(mqtt PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    mqtt
    PUBLIC "${TD_SOURCE_DIR}/include/libs/tmqtt"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/tmqtt/inc"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/tmqtt/mqtt/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/ttq/inc"
)
target_link_libraries(
    mqtt
    PRIVATE util common ${TAOS_NATIVE_LIB_STATIC}
)

endif()
