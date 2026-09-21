aux_source_directory(src MQTT_SRC)
aux_source_directory(ttq/src MQTT_TTQ_SRC)

if(TD_LINUX OR TD_WINDOWS)

add_library(mqtt STATIC ${MQTT_SRC} ${MQTT_TTQ_SRC})

if(TD_LINUX)
    find_path(HAVE_SYS_EPOLL_H sys/epoll.h)
    if (HAVE_SYS_EPOLL_H)
        add_definitions("-DWITH_EPOLL")
    endif()
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

DEP_ext_libuv_INC(mqtt)

# libmqtt/ttq never touches the engine: it reaches TDengine only through the
# tmq_ctx_* entry points the taosmqtt executable implements (its objects have no
# undefined symbol that libtaosnative.so defines).  A static library's PRIVATE
# dependencies are still usage requirements for its consumers, so naming the
# archives here put the whole engine stack on taosmqtt's link line, where the
# linker prefers them over libtaosnative.so and embeds a second copy of the
# engine.
#
# The include paths are still needed though: tmqttCtx.h (pulled in through
# ttq.h) includes taos.h, thash.h and tmisce.h.  libtaos.so re-advertises all of
# them (PUBLIC include/client plus os/util collected as INTERFACE include
# directories), and being a shared library it cannot duplicate any code.
#
# Windows keeps the archives: its dispatcher is still fat, and there the
# previous setup is what the other Windows tools use.
if(TD_WINDOWS)
    target_link_libraries(
        mqtt
        PRIVATE util common ${TAOS_NATIVE_LIB_STATIC}
    )
    target_link_libraries(mqtt PRIVATE ws2_32)
else()
    target_link_libraries(mqtt PRIVATE ${TAOS_LIB})
endif()

endif()
